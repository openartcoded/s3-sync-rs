#![allow(dead_code)]
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
use time::{macros::format_description, UtcOffset};
use tracing_subscriber::fmt::time::OffsetTime;

struct S3FileEntry {
    key: String,
    path_buf: PathBuf,
    created: SystemTime,
}
#[derive(Debug)]
struct S3Config {
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
    let bucket = var("S3_BUCKET").unwrap_or_else(|_| "artcoded".into());
    let number_entry_to_keep_in_zone = var("S3_NUM_ENTRIES_TO_KEEP_IN_ZONE")
        .ok()
        .and_then(|tick| tick.parse::<i64>().ok())
        .unwrap_or(1);
    let directory = var("S3_DIRECTORY").unwrap_or_else(|_| "/tmp/test".into());
    let endpoint = var("S3_ENDPOINT").unwrap_or_else(|_| "https://s3.fr-par.scw.cloud".into());
    let region = var("S3_REGION").unwrap_or_else(|_| "fr-par".into());
    let cron_expression = var("S3_CRON_EXPRESSION").unwrap_or_else(|_| "0 0 4 * * * *".into());
    let chunk_size = var("S3_CHUNK_SIZE")
        .ok()
        .and_then(|tick| tick.parse::<u64>().ok())
        .unwrap_or(1024 * 1024 * 5);
    let max_chunks = var("S3_MAX_CHUNKS")
        .ok()
        .and_then(|tick| tick.parse::<u64>().ok())
        .unwrap_or(1024);

    let directory_path = PathBuf::from(&directory);

    fn check_directory(directory: &str, directory_path: &Path) -> Result<(), Box<dyn Error>> {
        if !directory_path.exists() || !directory_path.is_dir() {
            return Err(Box::new(SyncError::InvalidDirectory(directory.to_string())));
        }
        Ok(())
    }

    check_directory(&directory, &directory_path)?;

    let shared_config = aws_config::from_env().load().await;
    let config = aws_sdk_s3::config::Builder::from(&shared_config)
        .endpoint_url(endpoint)
        .region(Region::new(region))
        .build();

    let client = aws_sdk_s3::Client::from_conf(config);

    tracing::info!("check if bucket exists...");

    if let error @ Err(_) = bucket_exists(&client, &bucket).await {
        return error;
    }
    let s3_config = S3Config {
        cron_expression,
        chunk_size,
        max_chunks,
        number_entry_to_keep_in_zone,
        directory_path,
        bucket,
    };

    tracing::info!("starting...");
    run(&client, &s3_config).await?;
    Ok(())
}

async fn run(client: &Client, config: &S3Config) -> Result<(), Box<dyn Error>> {
    let schedule = Schedule::from_str(&config.cron_expression)?;
    tracing::info!("started!");
    for next in schedule.upcoming(chrono::Local) {
        tracing::info!("next schedule {next}");
        let now = Local::now();
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
        let mut entries = vec![];

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
        tracing::info!("{count_entries} entries in directory.");
        let number_entries_to_glacier = count_entries - config.number_entry_to_keep_in_zone;
        for (
            index,
            S3FileEntry {
                key,
                path_buf,
                created: _,
            },
        ) in entries.into_iter().enumerate()
        {
            let index = index as i64;
            tracing::info!(
                "processing file {}/{} with name {key}",
                index + 1,
                count_entries
            );
            match object_storage_class(&client, &key, &config.bucket).await {
                Some(storage_class) => {
                    tracing::debug!("file seems to exists, check storage class...");
                    if config.number_entry_to_keep_in_zone < 0 {
                        tracing::info!("retention policy set to keep all archives out of glacier.");
                    } else if number_entries_to_glacier > index {
                        update_storage_class_to_glacier(
                            &client,
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
                        &client,
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
    }
    Ok(())
}

// functions

type S3Object = (String, ObjectStorageClass);
async fn list_objects(client: &Client, bucket: &str) -> Result<Vec<S3Object>, Box<dyn Error>> {
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
async fn delete_object(
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
        }
    }
}

impl Error for SyncError {}
