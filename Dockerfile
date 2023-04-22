
FROM rust:1.69 as builder

WORKDIR /app

RUN cargo new s3-sync-rs

WORKDIR /app/s3-sync-rs

COPY ./Cargo.toml ./Cargo.lock ./

RUN cargo build --release 

RUN rm -rf ./src

COPY ./src/ ./src

RUN rm ./target/release/deps/s3_sync_rs*

RUN cargo build --release 

FROM debian:bullseye-slim AS runtime
RUN apt  update && apt upgrade -y
RUN apt install -y ca-certificates 

# Set timezone
ENV TZ="Europe/Brussels"

ENV RUST_LOG=info

VOLUME /root/.local/share

COPY --from=builder  /app/s3-sync-rs/target/release/s3-sync-rs .

ENTRYPOINT [ "./s3-sync-rs" ]
