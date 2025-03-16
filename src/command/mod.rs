use ping::Ping;

use crate::parse::Parse;
use crate::server::Shutdown;
use crate::store::Db;
use crate::{Connection, Frame};

pub mod echo;
pub mod get;
pub mod ping;
pub mod set;
pub mod unknown;
pub use echo::Echo;
pub use unknown::Unknown;
pub use get::Get;
pub use set::Set;

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    Echo(Echo),
    Get(Get),
    Set(Set),
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let mut parse = Parse::new(frame)?;
        let command_string = parse.next_string()?.to_lowercase();

        let command = match &command_string[..] {
            "ping" => Command::Ping(Ping::parse_frame(&mut parse)?),
            "echo" => Command::Echo(Echo::parse_frame(&mut parse)?),
            "get" => Command::Get(Get::parse_frame(&mut parse)?),
            "set" => Command::Set(Set::parse_frame(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_string)));
            }
        };
        parse.finish()?;
        Ok(command)
    }

    pub async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        _shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;
        match self {
            Ping(cmd) => cmd.apply(dst).await,
            Echo(cmd) => cmd.apply(dst).await,
            Get(cmd)=> cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
        }
    }
}
