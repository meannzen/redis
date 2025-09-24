pub mod command;
pub mod server_cli;
pub mod connection;
pub mod database;
pub mod frame;
pub mod parse;
pub mod server;
pub mod store;
pub use command::Command;
pub use connection::Connection;
pub use frame::Frame;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub const DEFAULT_PORT: u16 = 6379;
