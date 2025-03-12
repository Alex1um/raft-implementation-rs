use std::net::Ipv4Addr;

pub const TIMEOUT: f32 = 3.;
pub const INTERVAL: f32 = 1.;
pub const MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(224, 2, 2, 4);
pub const MULTICAST_PORT: u16 = 48666;
pub const BIND_ADDR: Ipv4Addr = Ipv4Addr::new(0, 0, 0, 0);
pub const BIND_PORT: u16 = 48666;
