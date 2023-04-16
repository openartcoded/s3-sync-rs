#![allow(dead_code)]
use std::{
    env::var,
    error::Error,
    fmt::Display,
    path::{Path, PathBuf},
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

struct S3FileEntry {
    key: String,
    path_buf: PathBuf,
    created: SystemTime,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let bucket = var("S3_BUCKET").unwrap_or_else(|_| "artcoded".into());
    let number_entry_to_keep_in_zone = var("S3_NUM_ENTRIES_TO_KEEP_IN_ZONE")
        .ok()
        .and_then(|tick| tick.parse::<i64>().ok())
        .unwrap_or(1);
    let directory = var("S3_DIRECTORY").unwrap_or_else(|_| "/tmp/test".into());
    let endpoint = var("S3_ENDPOINT").unwrap_or_else(|_| "https://s3.fr-par.scw.cloud".into());
    let region = var("S3_REGION").unwrap_or_else(|_| "fr-par".into());
    let interval_in_minutes = var("S3_INTERVAL_IN_MINUTES")
        .ok()
        .and_then(|tick| tick.parse::<u64>().ok())
        .unwrap_or(1);
    let chunk_size = var("S3_CHUNK_SIZE")
        .ok()
        .and_then(|tick| tick.parse::<u64>().ok())
        .unwrap_or(1024 * 1024 * 5);
    let max_chunks = var("S3_MAX_CHUNKS")
        .ok()
        .and_then(|tick| tick.parse::<u64>().ok())
        .unwrap_or(1024);

    let directory = PathBuf::from(directory);

    if !directory.exists() || !directory.is_dir() {
        panic!("{directory:?} doesn't exist or is not a directory");
    }

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

    tracing::info!("starting...");

    loop {
        let mut dir = tokio::fs::read_dir(&directory).await?;
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
        let number_entries_to_glacier = count_entries - number_entry_to_keep_in_zone;
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
                "processing filename {}/{} with name {key}",
                index + 1,
                count_entries
            );
            match object_storage_class(&client, &key, &bucket).await {
                Some(storage_class) => {
                    tracing::debug!("file seems to exists, check storage class...");
                    if number_entries_to_glacier > index {
                        update_storage_class_to_glacier(&client, &key, &storage_class, &bucket)
                            .await?;
                    } else {
                        tracing::info!("file exists but retention policy set to keep {number_entry_to_keep_in_zone} in zone ");
                    }
                }
                None => {
                    tracing::info!("pushing new file to s3");
                    upload(
                        &client,
                        path_buf.as_path(),
                        StorageClass::OnezoneIa,
                        chunk_size,
                        max_chunks,
                        &bucket,
                    )
                    .await?;
                }
            }
        }

        tracing::info!("wait till next tick...");
        tokio::time::sleep(Duration::from_secs(interval_in_minutes * 60)).await;
    }
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
            Err(Box::new(SyncError::BucketDoesNotExist))
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
        | StorageClass::IntelligentTiering => todo!(),
        StorageClass::ReducedRedundancy => {
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
    BucketDoesNotExist,
}
impl Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadFileSize => write!(f, "bad file size"),
            Self::TooManyChunks => write!(f, "too many chunks"),
            Self::BucketDoesNotExist => write!(f, "bucketdoes not exist!"),
        }
    }
}

impl Error for SyncError {}
