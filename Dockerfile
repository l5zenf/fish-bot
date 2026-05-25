FROM rust:1-bookworm AS builder

ARG BIN=fish-example-quickstart-simple
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release -p "${BIN}" \
    && cp "target/release/${BIN}" /app/fish-bot

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/fish-bot /app/fish-bot

CMD ["/app/fish-bot"]
