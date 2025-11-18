use crate::{
    frame::Frame,
    geometry::{validate_geo_coordinates, Coordinates},
    parse::Parse,
    store::Db,
    Connection,
};

#[derive(Debug)]
pub struct GeoAdd {
    key: String,
    coordinate: Coordinates,
    member: String,
}

impl GeoAdd {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<GeoAdd> {
        let key = parse.next_string()?;
        let longitude: f64 = parse.next_string()?.parse()?;
        let latitude: f64 = parse.next_string()?.parse()?;
        let member = parse.next_string()?;

        Ok(GeoAdd {
            key,
            coordinate: Coordinates {
                latitude,
                longitude,
            },
            member,
        })
    }

    pub async fn apply(self, _db: &Db, conn: &mut Connection) -> crate::Result<()> {
        dbg!(self.key, self.member);
        let frame =
            match validate_geo_coordinates(self.coordinate.longitude, self.coordinate.latitude) {
                Ok(_) => Frame::Integer(1),
                Err(s) => Frame::Error(s.to_string()),
            };

        conn.write_frame(&frame).await?;
        Ok(())
    }
}
