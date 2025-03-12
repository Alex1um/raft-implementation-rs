FROM scratch

WORKDIR /

COPY target/x86_64-unknown-linux-musl/release/raft-implementation-rs /raft-implementation-rs

CMD ["/raft-implementation-rs"]

