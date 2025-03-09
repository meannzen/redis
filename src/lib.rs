mod connection;
pub mod frame;
pub use connection::Connection;
pub use frame::Frame;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
