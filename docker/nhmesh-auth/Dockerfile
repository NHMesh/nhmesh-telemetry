FROM rust AS builder

COPY ./nhmesh-mqtt-auth ./nhmesh-mqtt-auth

WORKDIR /nhmesh-mqtt-auth

RUN cargo build --release

FROM debian:trixie-slim

RUN mkdir -p /app/bin
WORKDIR /app

RUN apt-get update && apt-get install -y ca-certificates

COPY --from=builder /nhmesh-mqtt-auth/target/release/nhmesh-mqtt-auth /app/bin/nhmesh-mqtt-auth
COPY --from=builder /nhmesh-mqtt-auth/config.yaml /app/config.yaml

EXPOSE 8000

CMD ["/app/bin/nhmesh-mqtt-auth"]