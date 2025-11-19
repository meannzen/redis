use bytes::Bytes;

use crate::{
    frame::Frame,
    geometry::{decode, encode, validate_geo_coordinates, Coordinates},
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

#[derive(Debug)]
pub struct GeoPos {
    key: String,
    members: Vec<Bytes>,
}

#[derive(Debug)]
pub struct GeoDist {
    key: String,
    from: Bytes,
    to: Bytes,
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

impl GeoPos {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<GeoPos> {
        let key = parse.next_string()?;
        let mut members = vec![];

        while let Ok(bytes) = parse.next_bytes() {
            members.push(bytes);
        }

        Ok(GeoPos { key, members })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let values = db.gpos(self.key, self.members);

        let mut positions = vec![];
        for value in values {
            if let Some(score) = value {
                let coord = decode(score as u64);
                positions.push(Some((coord.longitude, coord.latitude)));
            } else {
                positions.push(None);
            }
        }

        conn.write_geopos(positions).await?;
        Ok(())
    }
}

impl GeoDist {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<GeoDist> {
        let key = parse.next_string()?;
        let from = parse.next_bytes()?;
        let to = parse.next_bytes()?;

        Ok(GeoDist { key, from, to })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let from_coordinate = db.zscore(self.key.clone(), self.from);
        let to_coordinate = db.zscore(self.key, self.to);

        let mut frame = Frame::Null;
        if let (Some(from_raw), Some(to_raw)) = (from_coordinate, to_coordinate) {
            let from = decode(from_raw as u64);
            let to = decode(to_raw as u64);
            let distance = from.haversine_distance(&to);
            frame = Frame::Bulk(Bytes::from(distance.to_string()));
        }

        conn.write_frame(&frame).await?;
        Ok(())
    }
}
