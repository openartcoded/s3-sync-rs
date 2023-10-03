# s3-sync-rs

Sync a given folder with S3.

If used with [SmsGatewayAndroid](https://github.com/openartcoded/sms-gateway-android) and [ArtemisMQ](https://github.com/openartcoded/activemq-artemis),
it can send an sms when a job ran successfully. It uses MQTT protocol to transmit the sms request to the android phone that sends the sms.

## Usage

```yaml
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
      # sms notification
      PHONE_NUMBER: "+32488112233" # must be valid
      MQTT_ENABLED: "true" # must be set to true
      MQTT_TOPIC_PUBLISHING: "sms" # topic to publish sms request, default set to sms
      MQTT_HOST: "artemis" # hostname, default set to 127.0.0.1
      MQTT_PORT: 1883 # port, default to 1883
      MQTT_CLIENT_ID: "s3_mqtt_subscriber" # client id, can be something else
      MQTT_USERNAME: "root"
      MQTT_PASSWORD: "root"
```
