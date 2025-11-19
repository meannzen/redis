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

#[derive(Debug)]
pub struct GeoSearch {
    key: String,
    center_lon: f64,
    center_lat: f64,
    radius: f64,
    unit: String,
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

impl GeoSearch {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<GeoSearch> {
        let key = parse.next_string()?;

        let _ = parse.next_string()?;
        let center_lon = parse.next_string()?.parse()?;
        let center_lat = parse.next_string()?.parse()?;

        let _ = parse.next_string()?;
        let radius = parse.next_string()?.parse()?;
        let unit = parse.next_string()?.to_lowercase();

        Ok(GeoSearch {
            key,
            center_lon,
            center_lat,
            radius,
            unit,
        })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let radius_meters = match self.unit.as_str() {
            "m" => self.radius,
            "km" => self.radius * 1_000.0,
            "mi" => self.radius * 1609.344,
            "ft" => self.radius * 0.3048,
            _ => {
                conn.write_frame(&Frame::Error("ERR unsupported unit".into()))
                    .await?;
                return Ok(());
            }
        };

        let center = Coordinates::new(self.center_lat, self.center_lon);

        let mut results = Frame::array();
        let valuse = db.gsearch(self.key, center, radius_meters);

        for bytes in valuse {
            results.push_bulk(bytes);
        }

        conn.write_frame(&results).await?;
        Ok(())
    }
}
