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
   S3_INTERVAL_IN_MINUTES: 1
   AWS_ACCESS_KEY_ID: "..."
   AWS_SECRET_ACCESS_KEY: "..."
```
