# s3-sync-rs

Sync a given folder with S3.

## Usage

```
s3-sync-dump:
  image: nbittich/s3-sync-rs
  networks:
    artcoded:
  volumes:
    - ./data/backend/dump:/var/artcoded/data
  environment:
   S3_BUCKET: "bucket-name"
   S3_DIRECTORY: /var/artcoded/data
   S3_ENDPOINT: "https://s3.fr-par.scw.cloud"
   S3_REGION: "fr-par"
   S3_CRON_EXPRESSION: 0 0 4 * * * *
   S3_NUM_ENTRIES_TO_KEEP_IN_ZONE: 3 # keep the last three , if less than 0 keep all
   S3_CHUNK_SIZE: 5242880 # 5mb by default
   S3_MAX_CHUNKS: 1024 # 5mb 1024 =  5Gb
   AWS_ACCESS_KEY_ID: "..."
   AWS_SECRET_ACCESS_KEY: "..."
```
