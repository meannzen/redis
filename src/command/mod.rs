use crate::parse::Parse;
use crate::server::{ReplicaState, Shutdown, TransactionState};
use crate::store::Db;
use crate::{Connection, Frame};

pub mod config;
pub mod echo;
pub mod exec;
pub mod get;
pub mod incr;
pub mod info;
pub mod key;
pub mod multi;
pub mod ping;
pub mod psync;
pub mod replconf;
pub mod set;
pub mod type_cmd;
pub mod unknown;
pub mod wait;
pub mod xadd;
pub mod xrange;
pub mod xread;
pub use config::Config;
pub use echo::Echo;
pub use exec::Exec;
pub use get::Get;
pub use incr::Incr;
pub use info::Info;
pub use key::Keys;
pub use multi::Multi;
pub use ping::Ping;
pub use psync::PSync;
pub use replconf::ReplConf;
pub use set::Set;
pub use type_cmd::Type;
pub use unknown::Unknown;
pub use wait::Wait;
pub use xadd::XAdd;
pub use xrange::XRange;
pub use xread::XRead;

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    Echo(Echo),
    Get(Get),
    Set(Set),
    Config(Config),
    Keys(Keys),
    Info(Info),
    ReplConf(ReplConf),
    PSync(PSync),
    Wait(Wait),
    Type(Type),
    XAdd(XAdd),
    XRange(XRange),
    XRead(XRead),
    Ince(Incr),
    Muiti(Multi),
    Exec(Exec),
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
            "keys" => Command::Keys(Keys::parse_frame(&mut parse)?),
            "info" => Command::Info(Info::parse_frame(&mut parse)?),
            "replconf" => Command::ReplConf(ReplConf::parse_frame(&mut parse)?),
            "psync" => Command::PSync(PSync::parse_frame(&mut parse)?),
            "wait" => Command::Wait(Wait::parse_frame(&mut parse)?),
            "type" => Command::Type(Type::parse_frame(&mut parse)?),
            "xadd" => Command::XAdd(XAdd::parse_frame(&mut parse)?),
            "xrange" => Command::XRange(XRange::parse_frame(&mut parse)?),
            "xread" => Command::XRead(XRead::parse_frame(&mut parse)?),
            "incr" => Command::Ince(Incr::parse_frame(&mut parse)?),
            "multi" => Command::Muiti(Multi::parse_frame(&mut parse)?),
            "exec" => Command::Exec(Exec::parse_frame(&mut parse)?),
            "config" => {
                let sub_command_string = parse.next_string()?.to_lowercase();
                match &sub_command_string[..] {
                    "get" => Command::Config(Config::parse_frame(&mut parse)?),
                    _ => {
                        return Ok(Command::Unknown(Unknown::new(sub_command_string)));
                    }
                }
            }
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_string)));
            }
        };
        parse.finish()?;
        Ok(command)
    }

    pub async fn apply(
        self,
        transaction_state: &TransactionState,
        replica_state: &ReplicaState,
        db: &Db,
        config: &crate::server_cli::Cli,
        conn: &mut Connection,
        _shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;
        match self {
            Ping(cmd) => cmd.apply(conn).await,
            Echo(cmd) => cmd.apply(conn).await,
            Get(cmd) => cmd.apply(db, conn).await,
            Set(cmd) => cmd.apply(db, conn).await,
            Config(cmd) => cmd.apply(config, conn).await,
            Keys(cmd) => cmd.apply(db, conn).await,
            Info(cmd) => cmd.apply(config, conn).await,
            ReplConf(cmd) => cmd.apply(conn, replica_state).await,
            PSync(cmd) => cmd.apply(conn, replica_state).await,
            Wait(cmd) => cmd.apply(conn, replica_state).await,
            Type(cmd) => cmd.apply(db, conn).await,
            XAdd(cmd) => cmd.apply(db, conn).await,
            XRange(cmd) => cmd.apply(db, conn).await,
            XRead(cmd) => cmd.apply(db, conn).await,
            Ince(cmd) => cmd.apply(db, conn).await,
            Muiti(cmd) => cmd.apply(transaction_state, conn).await,
            Exec(cmd) => cmd.apply(transaction_state, conn).await,
            Unknown(cmd) => cmd.apply(conn).await,
        }
    }

    pub fn is_writer(&self) -> bool {
        matches!(self, Command::Set(_))
    }
}
