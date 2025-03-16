use crate::{Connection, Frame};

#[derive(Debug)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    pub fn new(key: impl ToString) -> Self {
        Unknown {
            command_name: key.to_string(),
        }
    }

    pub fn get_name(&self) -> &str {
        &self.command_name
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let frame = Frame::Error(format!("ERR unknow command '{}'", self.command_name));
        dst.write_frame(&frame).await?;
        Ok(())
    }
}
