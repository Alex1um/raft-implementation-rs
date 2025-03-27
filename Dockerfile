FROM scratch

WORKDIR /

COPY target/x86_64-unknown-linux-musl/release/raft-implementation-rs /raft-implementation-rs

EXPOSE 8000

CMD ["/raft-implementation-rs"]

