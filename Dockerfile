FROM docker.io/rustlang/rust:nightly-bullseye-slim

WORKDIR /usr/local/src/sparql-delete-data-generator
COPY ./Cargo.toml ./
COPY ./src ./src
RUN cargo build --release
