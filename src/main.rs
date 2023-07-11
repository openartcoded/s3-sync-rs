mod mqtt_client;
use std::{
    env::var,
    error::Error,
    fmt::Display,
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, SystemTime},
};

use aws_sdk_s3::{
    config::Region,
    operation::create_multipart_upload::CreateMultipartUploadOutput,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart, ObjectStorageClass, StorageClass},
    Client,
};
use aws_smithy_http::byte_stream::Length;
use chrono::Local;
use cron::Schedule;
use paho_mqtt::{AsyncClient, Message, QOS_1};
use serde::{Deserialize, Serialize};
use time::{macros::format_description, UtcOffset};
use tokio::task::JoinSet;
use tracing::{error, info};
use tracing_subscriber::fmt::time::OffsetTime;

use crate::mqtt_client::{MQTT_ENABLED, TOPIC_PUBLISHING};

lazy_static::lazy_static! {
   static ref S3_CONFIG_FILE: PathBuf = PathBuf::from_str(
        &var("S3_CONFIG_FILE_PATH").unwrap_or_else(|_| "/var/config/configs.json".into()),
    ).expect("S3_CONFIG_FILE_PATH not found");

    static ref S3_ENDPOINT: String = var("S3_ENDPOINT").unwrap_or_else(|_| "https://s3.fr-par.scw.cloud".into());
    static ref S3_REGION: String  = var("S3_REGION").unwrap_or_else(|_| "fr-par".into());

    static ref PHONE_NUMBER: Option<String> =  var("PHONE_NUMBER").ok();
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Sms {
    phone_number: String,
    message: String,
}

struct S3FileEntry {
    key: String,
    path_buf: PathBuf,
    created: SystemTime,
}
#[derive(Debug, Deserialize)]
struct S3Config {
    title: String,
    cron_expression: String,
    bucket: String,
    chunk_size: u64,
    max_chunks: u64,
    number_entry_to_keep_in_zone: i64,
    directory_path: PathBuf,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let offset_hours = {
        let now = Local::now();
        let offset_seconds = now.offset().local_minus_utc();
        let hours = offset_seconds / 3600;
        hours as i8
    };
    let offset = UtcOffset::from_hms(offset_hours, 0, 0)?;

    let timer = OffsetTime::new(
        offset,
        format_description!("[day]-[month]-[year] [hour]:[minute]:[second]"),
    );
    tracing_subscriber::fmt().with_timer(timer).init();

    let configs: Vec<S3Config> =
        serde_json::from_str(&std::fs::read_to_string(S3_CONFIG_FILE.clone())?)?;

    fn check_directory(directory_path: &Path) -> Result<(), Box<dyn Error>> {
        if !directory_path.exists() || !directory_path.is_dir() {
            return Err(Box::new(SyncError::InvalidDirectory(
                directory_path.to_string_lossy().to_string(),
            )));
        }
        Ok(())
    }

    let shared_config = aws_config::from_env().load().await;
    let config = aws_sdk_s3::config::Builder::from(&shared_config)
        .endpoint_url(S3_ENDPOINT.clone())
        .region(Region::new(S3_REGION.clone()))
        .build();

    let client = aws_sdk_s3::Client::from_conf(config);

    let mq_client = if *MQTT_ENABLED {
        let cli = mqtt_client::Client::new(Default::default())?;
        let cli = cli
            .connect(|_, _| tracing::info!("connected to mqtt!"))
            .await?;
        Some(cli)
    } else {
        None
    };

    let mut tasks = JoinSet::new();
    tracing::info!("initialize {} tasks...", configs.len());
    for config in configs {
        tracing::info!("check cron expression... {}", config.cron_expression);
        let _ = Schedule::from_str(&config.cron_expression)?;
        tracing::info!("check if directory {:?} exists...", config.directory_path);
        check_directory(&config.directory_path)?;
        tracing::info!("check if bucket {} exists...", config.bucket);

        let mq_client = mq_client.clone();
        let client = client.clone();
        if let error @ Err(_) = bucket_exists(&client, &config.bucket).await {
            return error;
        }
        tasks.spawn(async move {
            run(&client, &config, &mq_client)
                .await
                .map_err(|e| SyncError::TaskError(e.to_string()))?;
            Ok(()) as Result<(), SyncError>
        });
    }

    tracing::info!("starting...");
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(_)) => {}
            e => {
                tracing::error!("join error {e:?}");
            }
        }
    }

    if let Some(mq_cli) = mq_client {
        mq_cli.disconnect(None).await?;
    }

    Ok(())
}

async fn run(
    client: &Client,
    config: &S3Config,
    mq_cli: &Option<AsyncClient>,
) -> Result<(), Box<dyn Error>> {
    let schedule = Schedule::from_str(&config.cron_expression)?;
    tracing::info!("running task with title '{}'", config.title);
    let mut now;
    let mut entries = vec![];
    for next in schedule.upcoming(chrono::Local) {
        now = Local::now();

        tracing::info!("next schedule for '{}': {next}", config.title);
        if now < next {
            let duration = next - now;
            tracing::info!(
                "waiting {} hour(s) {} minute(s) {} second(s) before next run...",
                duration.num_hours(),
                duration.num_minutes() % 60,
                duration.num_seconds() % 3600 % 60
            );
            tokio::time::sleep(Duration::from_millis(duration.num_milliseconds() as u64)).await;
        }
        tracing::info!("running...");
        let mut dir = tokio::fs::read_dir(&config.directory_path).await?;

        while let Ok(Some(entry)) = dir.next_entry().await {
            let key = entry.file_name();
            let key = key.to_str().ok_or("file name error")?;
            let path_buf = entry.path();
            let created = entry.metadata().await?.created()?;
            entries.push(S3FileEntry {
                key: key.to_string(),
                path_buf,
                created,
            });
        }
        entries.sort_by(|s1, s2| s1.created.cmp(&s2.created));

        let count_entries = entries.len() as i64;
        tracing::info!(
            "{count_entries} entries in {:?} directory.",
            config.directory_path
        );
        let number_entries_to_glacier = count_entries - config.number_entry_to_keep_in_zone;
        for (
            index,
            S3FileEntry {
                key,
                path_buf,
                created: _,
            },
        ) in entries.drain(..).enumerate()
        {
            let index = index as i64;
            tracing::info!(
                "processing file {}/{} with name {key}",
                index + 1,
                count_entries
            );

            match object_storage_class(client, &key, &config.bucket).await {
                Some(storage_class) => {
                    tracing::debug!("file seems to exists, check storage class...");
                    if config.number_entry_to_keep_in_zone < 0 {
                        tracing::info!("retention policy set to keep all archives out of glacier.");
                    } else if number_entries_to_glacier > index {
                        update_storage_class_to_glacier(
                            client,
                            &key,
                            &storage_class,
                            &config.bucket,
                        )
                        .await?;
                    } else {
                        tracing::info!(
                            "file exists but retention policy set to keep {} in zone ",
                            config.number_entry_to_keep_in_zone
                        );
                    }
                }
                None => {
                    tracing::info!("pushing new file to s3");
                    upload(
                        client,
                        path_buf.as_path(),
                        StorageClass::OnezoneIa,
                        config.chunk_size,
                        config.max_chunks,
                        &config.bucket,
                    )
                    .await?;
                }
            }
        }
        if let (Some(mq_cli), Some(phone_number)) = (mq_cli, PHONE_NUMBER.clone()) {
            tracing::info!("send sms for job {}", config.title);
            let mut is_err = false;
            if !mq_cli.is_connected() {
                match mq_cli.reconnect().await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("could not reconnect to mqtt broker...{e}");
                        is_err = true;
                    }
                }
            }
            if !is_err {
                match mq_cli
                    .publish(Message::new(
                        TOPIC_PUBLISHING.clone(),
                        serde_json::to_vec(&Sms {
                            message: format!(
                                "s3 sync: job '{}' ran successfully at {}",
                                config.title, next
                            ),
                            phone_number,
                        })?,
                        QOS_1,
                    ))
                    .await
                {
                    Ok(_) => info!("sms sent"),
                    Err(e) => error!("could not send sms... {e}"),
                }
            }
        }
    }

    Ok(())
}

// functions

type _S3Object = (String, ObjectStorageClass);
async fn _list_objects(client: &Client, bucket: &str) -> Result<Vec<_S3Object>, Box<dyn Error>> {
    let resp = client.list_objects().bucket(bucket).send().await?;
    let mut objects = vec![];
    for object in resp.contents().unwrap_or_default() {
        match (object.key(), object.storage_class()) {
            (Some(key), Some(storage_class)) => objects.push((key.into(), storage_class.clone())),
            _ => tracing::warn!(
                "could not determine key or storage class. key: {:?}, storage_class: {:?}",
                object.key(),
                object.storage_class
            ),
        }
        println!("{}", object.key().unwrap_or_default());
    }

    Ok(objects)
}
async fn object_storage_class(
    client: &Client,
    file_name: &str,
    bucket: &str,
) -> Option<StorageClass> {
    match client
        .head_object()
        .key(file_name)
        .bucket(bucket)
        .send()
        .await
    {
        Ok(metadata) => metadata.storage_class().cloned(),
        Err(e) => {
            tracing::debug!("object doesn't seem to exist: err {e:?}");
            None
        }
    }
}
async fn _delete_object(
    client: &Client,
    file_name: &str,
    bucket: &str,
) -> Result<(), Box<dyn Error>> {
    client
        .delete_object()
        .key(file_name)
        .bucket(bucket)
        .send()
        .await?;
    Ok(())
}
async fn bucket_exists(client: &Client, bucket: &str) -> Result<(), Box<dyn Error>> {
    match client.head_bucket().bucket(bucket).send().await {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::debug!("bucket exists err {e:?}");
            Err(Box::new(SyncError::BucketDoesNotExist(bucket.into())))
        }
    }
}
async fn update_storage_class_to_glacier(
    client: &Client,
    key: &str,
    old_storage_class: &StorageClass,
    bucket: &str,
) -> Result<(), Box<dyn Error>> {
    match old_storage_class {
        StorageClass::DeepArchive | StorageClass::Glacier | StorageClass::GlacierIr => {
            tracing::debug!("{key} already in glacier")
        }
        StorageClass::Standard
        | StorageClass::StandardIa
        | StorageClass::Outposts
        | StorageClass::IntelligentTiering
        | StorageClass::ReducedRedundancy => {
            tracing::warn!("storage class not expected for {key}: {old_storage_class:?}")
        }
        StorageClass::Unknown(s) => {
            tracing::warn!("unknown storage class for {key}: {s:?}")
        }

        StorageClass::OnezoneIa => {
            tracing::info!("set class to glacier");
            client
                .copy_object()
                .key(key)
                .copy_source(format!("{bucket}/{key}"))
                .storage_class(StorageClass::Glacier)
                .bucket(bucket)
                .send()
                .await?;
        }
        _ => tracing::warn!("unknown new storage class for {key}: {old_storage_class:?}"),
    }
    Ok(())
}
async fn upload(
    client: &Client,
    file_path: &Path,
    storage_class: StorageClass,
    chunk_size: u64,
    max_chunks: u64,
    bucket: &str,
) -> Result<(), Box<dyn Error>> {
    let file_size = tokio::fs::metadata(file_path).await?.len();
    let file_name = file_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or("couldn't determine filename")?;

    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(file_name)
        .storage_class(storage_class)
        .send()
        .await?;
    let upload_id = multipart_upload_res
        .upload_id()
        .ok_or("could not get upload id")?;
    let mut chunk_count = (file_size / chunk_size) + 1;
    let mut size_of_last_chunk = file_size % chunk_size;
    if size_of_last_chunk == 0 {
        size_of_last_chunk = chunk_size;
        chunk_count -= 1;
    }

    if file_size == 0 {
        return Err(Box::new(SyncError::BadFileSize));
    }
    if chunk_count > max_chunks {
        return Err(Box::new(SyncError::TooManyChunks));
    }
    let mut upload_parts: Vec<CompletedPart> = vec![];
    for chunk_index in 0..chunk_count {
        tracing::info!("processing part {}/{chunk_count}...", chunk_index + 1);
        let this_chunk = if chunk_count - 1 == chunk_index {
            size_of_last_chunk
        } else {
            chunk_size
        };
        let stream = ByteStream::read_from()
            .path(file_path)
            .offset(chunk_index * chunk_size)
            .length(Length::Exact(this_chunk))
            .build()
            .await?;
        let part_number = (chunk_index as i32) + 1;
        let upload_part_res = client
            .upload_part()
            .key(file_name)
            .bucket(bucket)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await?;
        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );
    }
    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();
    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(file_name)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await?;
    Ok(())
}

#[derive(Debug)]
enum SyncError {
    BadFileSize,
    TooManyChunks,
    BucketDoesNotExist(String),
    InvalidDirectory(String),
    TaskError(String),
}
impl Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadFileSize => write!(f, "bad file size"),
            Self::TooManyChunks => write!(f, "too many chunks"),
            Self::BucketDoesNotExist(b) => write!(f, "bucket {b} does not exist!"),
            Self::InvalidDirectory(directory) => {
                write!(f, "{directory} doesn't exist or is not a directory")
            }
            Self::TaskError(e) => write!(f, "task error {e}"),
        }
    }
}

impl Error for SyncError {}
