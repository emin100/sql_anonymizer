FROM --platform=linux/amd64 rust:1.72.1-bullseye

RUN apt update && apt install gcc
RUN rustup target add x86_64-unknown-linux-gnu
#RUN cargo build --release --target=x86_64-unknown-linux-gnu

RUN rustup default 1.72.1-x86_64-unknown-linux-gnu
