use std::env;

#[derive(Debug)]
pub struct Config {
    pub dir: Option<String>,
    pub db_file_name: Option<String>,
}

impl Config {
    pub fn file_path(&self) -> Option<String> {
        let file_name = match &self.db_file_name {
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
}

impl From<std::env::Args> for Config {
    fn from(mut value: std::env::Args) -> Self {
        let mut dir: Option<String> = None;
        let mut db_file_name: Option<String> = None;
        while let Some(v) = value.next() {
            if v == "--dir" {
                dir = value.next();
            }
            if v == "--dbfilename" {
                db_file_name = value.next();
            }
        }

        Self { dir, db_file_name }
    }
}
