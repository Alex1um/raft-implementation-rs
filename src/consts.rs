use std::{net::Ipv4Addr, time::Duration};

pub const TIMEOUT: f32 = 3.;
pub const INTERVAL: f32 = 1.;
pub const MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(224, 2, 2, 4);
pub const MULTICAST_PORT: u16 = 48666;
pub const BIND_ADDR: Ipv4Addr = Ipv4Addr::new(0, 0, 0, 0);
pub const BIND_PORT: u16 = 48666;
pub const MIN_ELECTION_TIMEOUT: Duration = Duration::from_millis(500);
pub const MAX_ELECTION_TIMEOUT: Duration = Duration::from_millis(700);
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
pub const LOCK_TIMEOUT: Duration = Duration::from_secs(30);
pub const MIN_NODES: usize = 3;
