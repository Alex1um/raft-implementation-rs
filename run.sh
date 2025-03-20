cargo build --release --target=x86_64-unknown-linux-musl
sudo docker compose -f docker-compose.yaml up --build
