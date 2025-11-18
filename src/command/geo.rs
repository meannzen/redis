use bytes::Bytes;

use crate::{
    frame::Frame,
    geometry::{encode, validate_geo_coordinates, Coordinates},
    parse::Parse,
    store::Db,
    Connection,
};

#[derive(Debug)]
pub struct GeoAdd {
    key: String,
    coordinate: Coordinates,
    member: Bytes,
}

impl GeoAdd {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<GeoAdd> {
        let key = parse.next_string()?;
        let longitude: f64 = parse.next_string()?.parse()?;
        let latitude: f64 = parse.next_string()?.parse()?;
        let member = parse.next_bytes()?;

        Ok(GeoAdd {
            key,
            coordinate: Coordinates {
                latitude,
                longitude,
            },
            member,
        })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let frame =
            match validate_geo_coordinates(self.coordinate.longitude, self.coordinate.latitude) {
                Ok(_) => Frame::Integer(1),
                Err(s) => Frame::Error(s.to_string()),
            };

        db.zadd(
            self.key,
            self.member,
            encode(self.coordinate.latitude, self.coordinate.longitude) as f64,
        );

        conn.write_frame(&frame).await?;
        Ok(())
    }
}
