use clap::Parser;
use std::env;

use crate::DEFAULT_PORT;

#[derive(Parser, Debug)]
#[command(
    name = "rdis-server",
    version,
    author,
    about = "A redis config serverr"
)]
pub struct Cli {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long)]
    pub replicaof: Option<String>,
    #[arg(long)]
    pub dir: Option<String>,
    #[arg(long)]
    pub dbfilename: Option<String>,
}

impl Cli {
    pub fn file_path(&self) -> Option<String> {
        let file_name = match &self.dbfilename {
            Some(f) => f.as_str(),
            None => return None,
        };

        let dir_path = match &self.dir {
            Some(dir) => dir.clone(),
            None => env::current_dir()
                .ok()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| ".".to_string()),
        };

        Some(format!("{}/{}", dir_path.trim_end_matches('/'), file_name))
    }

    pub fn port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_PORT)
    }
}
