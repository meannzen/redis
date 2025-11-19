use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::{parse::Parse, store::Db, Connection, Frame};
#[derive(Debug)]
pub struct ACL {
    command: String,
    user: Option<String>,
    rule: Option<String>,
    password: Option<String>,
}

#[derive(Debug)]
pub struct Auth {
    username: String,
    password: String,
}

impl ACL {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ACL> {
        let command = parse.next_string()?;
        let user = parse.next_string().ok();
        let mut rule_pass = parse.next_string().unwrap_or("".to_string());
        let mut rule = None;
        let mut password = None;
        if rule_pass.starts_with(">") {
            rule = Some(">".to_string());
            password = Some(rule_pass.split_off(1));
        }

        Ok(ACL {
            command,
            user,
            rule,
            password,
        })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let command_str = self.command.to_lowercase();

        let frame = if command_str == "whoami" {
            Frame::Bulk(Bytes::from_static(b"default"))
        } else if command_str == "getuser" {
            let mut no_pass = vec![];
            let mut hash = vec![];
            if let Some(user) = self.user {
                if let Some(password_hash) = db.get_user_password_hash(&user) {
                    hash.push(Frame::Bulk(Bytes::from(password_hash)));
                } else {
                    no_pass.push(Frame::Bulk(Bytes::from_static(b"nopass")));
                }
            }
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"flags")),
                Frame::Array(no_pass),
                Frame::Bulk(Bytes::from_static(b"passwords")),
                Frame::Array(hash),
            ])
        } else if command_str == "setuser" {
            let mut frame = Frame::Null;
            if let (Some(rule), Some(password), Some(user)) = (self.rule, self.password, self.user)
            {
                if rule == ">" && !password.is_empty() && !user.is_empty() {
                    let hash = Sha256::digest(password);
                    let password_hash = hex::encode(hash);
                    db.insert_user(user, password_hash);
                    frame = Frame::Simple("OK".to_string());
                }
            }
            frame
        } else {
            Frame::Error(format!("Unknown command {}", command_str))
        };

        conn.write_frame(&frame).await?;
        Ok(())
    }
}

impl Auth {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Auth> {
        let username = parse.next_string()?;
        let password = parse.next_string()?;
        Ok(Auth { username, password })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let frame = if db.verify_user_passowrd(&self.username, self.password) {
            Frame::Simple("OK".to_string())
        } else {
            Frame::Error(
                "WRONGPASS invalid username-password pair or user is disabled.".to_string(),
            )
        };

        conn.write_frame(&frame).await?;
        Ok(())
    }
}
