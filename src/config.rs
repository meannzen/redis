#[derive(Debug)]
pub struct Config {
    pub dir: Option<String>,
    pub db_file_name: Option<String>,
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
