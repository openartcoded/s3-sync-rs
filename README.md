# s3-sync-rs

Sync a given folder with S3.

## Usage

```
version: "3.9"
services:
  s3-sync-dump:
     restart: unless-stopped
     build:
       context: .
     volumes:
         - ./config_example/configs.json:/var/s3/configs.json
         - ./data/backend/dump:/var/s3/dump
         - ./data/backend/dump-snapshot:/var/s3/dump-snapshot
         - ./data/backend/files:/var/s3/files
     environment:
         RUST_LOG: "info"
         S3_ENDPOINT: "<s3-endpoint>"
         S3_REGION: "<s3-region>"
         AWS_ACCESS_KEY_ID: "<key>"
         AWS_SECRET_ACCESS_KEY: "<secret>"
         S3_CONFIG_FILE_PATH: "/var/s3/configs.json"
```
