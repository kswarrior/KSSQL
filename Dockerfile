FROM rust:1.75-slim as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/ks-sql /app/ks-sql
COPY --from=builder /app/setup.sh /app/setup.sh
EXPOSE 5432 8080
CMD ["./ks-sql"]
