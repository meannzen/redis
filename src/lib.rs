pub mod clients;
pub mod command;
pub mod connection;
pub mod database;
pub mod frame;
pub mod parse;
pub mod server;
pub mod server_cli;
pub mod store;
pub use command::Command;
pub use connection::Connection;
pub use frame::Frame;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub const DEFAULT_PORT: u16 = 6380;
pub const MASTER_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
