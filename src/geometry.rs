use std::fmt;
const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;
// reference to source
/// https://github.com/codecrafters-io/redis-geocoding-algorithm/blob/main/rust/decode.rs
/// https://rosettacode.org/wiki/Haversine_formula#Rust

#[derive(Debug, Clone)]
pub struct Coordinates {
    pub latitude: f64,
    pub longitude: f64,
}

impl Coordinates {
    pub fn new(latitude: f64, longitude: f64) -> Coordinates {
        Coordinates {
            latitude,
            longitude,
        }
    }

    pub fn haversine(&self, other: Coordinates) -> f64 {
        haversine(self.clone(), other)
    }
}

fn haversine(origin: Coordinates, destination: Coordinates) -> f64 {
    const R: f64 = 6372.8;

    let lat1 = origin.latitude.to_radians();
    let lat2 = destination.latitude.to_radians();
    let d_lat = lat2 - lat1;
    let d_lon = (destination.longitude - origin.longitude).to_radians();

    let a = (d_lat / 2.0).sin().powi(2) + (d_lon / 2.0).sin().powi(2) * lat1.cos() * lat2.cos();
    let c = 2.0 * a.sqrt().asin();
    R * c
}

fn compact_int64_to_int32(v: u64) -> u32 {
    let mut result = v & 0x5555555555555555;
    result = (result | (result >> 1)) & 0x3333333333333333;
    result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result >> 4)) & 0x00FF00FF00FF00FF;
    result = (result | (result >> 8)) & 0x0000FFFF0000FFFF;
    ((result | (result >> 16)) & 0x00000000FFFFFFFF) as u32 // Cast to u32
}

fn convert_grid_numbers_to_coordinates(
    grid_latitude_number: u32,
    grid_longitude_number: u32,
) -> Coordinates {
    // Calculate the grid boundaries
    let grid_latitude_min =
        MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / 2.0_f64.powi(26));
    let grid_latitude_max =
        MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / 2.0_f64.powi(26));
    let grid_longitude_min =
        MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / 2.0_f64.powi(26));
    let grid_longitude_max =
        MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / 2.0_f64.powi(26));

    // Calculate the center point of the grid cell
    let latitude = (grid_latitude_min + grid_latitude_max) / 2.0;
    let longitude = (grid_longitude_min + grid_longitude_max) / 2.0;

    Coordinates {
        latitude,
        longitude,
    }
}

pub fn decode(geo_code: u64) -> Coordinates {
    // Align bits of both latitude and longitude to take even-numbered position
    let y = geo_code >> 1;
    let x = geo_code;

    // Compact bits back to 32-bit ints
    let grid_latitude_number = compact_int64_to_int32(x);
    let grid_longitude_number = compact_int64_to_int32(y);

    convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
}

#[derive(Debug)]
pub enum GeoError {
    InvalidLongitude(f64),
    InvalidLatitude(f64),
    InvalidPair(f64, f64),
}

impl fmt::Display for GeoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeoError::InvalidLongitude(_) => write!(f, "ERR invalid longitude"),
            GeoError::InvalidLatitude(_) => write!(f, "ERR invalid latitude"),
            GeoError::InvalidPair(lon, lat) => {
                write!(f, "ERR invalid longitude,latitude pair {lon},{lat}")
            }
        }
    }
}

pub fn validate_geo_coordinates(longitude: f64, latitude: f64) -> Result<(), GeoError> {
    if !longitude.is_finite() || !latitude.is_finite() {
        return Err(GeoError::InvalidPair(longitude, latitude));
    }

    if !(-180.0..=180.0).contains(&longitude) {
        return Err(GeoError::InvalidLongitude(longitude));
    }

    if !(-90.0..=90.0).contains(&latitude) {
        return Err(GeoError::InvalidLatitude(latitude));
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::{decode, haversine, Coordinates};

    #[test]
    fn test_haversine() {
        let origin: Coordinates = Coordinates {
            latitude: 36.12,
            longitude: -86.67,
        };
        let destination: Coordinates = Coordinates {
            latitude: 33.94,
            longitude: -118.4,
        };
        let d: f64 = haversine(origin, destination);
        println!("Distance: {} km ({} mi)", d, d / 1.609344);
        assert_eq!(d, 2887.2599506071106);
    }

    #[test]
    fn test_decode() {
        struct TestCase {
            name: &'static str,
            expected_latitude: f64,
            expected_longitude: f64,
            score: u64,
        }

        let test_cases = vec![
            TestCase {
                name: "Bangkok",
                expected_latitude: 13.722000686932997,
                expected_longitude: 100.52520006895065,
                score: 3962257306574459,
            },
            TestCase {
                name: "Beijing",
                expected_latitude: 39.9075003315814,
                expected_longitude: 116.39719873666763,
                score: 4069885364908765,
            },
            TestCase {
                name: "Berlin",
                expected_latitude: 52.52439934649943,
                expected_longitude: 13.410500586032867,
                score: 3673983964876493,
            },
            TestCase {
                name: "Copenhagen",
                expected_latitude: 55.67589927498264,
                expected_longitude: 12.56549745798111,
                score: 3685973395504349,
            },
            TestCase {
                name: "New Delhi",
                expected_latitude: 28.666698899347338,
                expected_longitude: 77.21670180559158,
                score: 3631527070936756,
            },
            TestCase {
                name: "Kathmandu",
                expected_latitude: 27.701700137333084,
                expected_longitude: 85.3205993771553,
                score: 3639507404773204,
            },
            TestCase {
                name: "London",
                expected_latitude: 51.50740077990134,
                expected_longitude: -0.12779921293258667,
                score: 2163557714755072,
            },
            TestCase {
                name: "New York",
                expected_latitude: 40.712798986951505,
                expected_longitude: -74.00600105524063,
                score: 1791873974549446,
            },
            TestCase {
                name: "Paris",
                expected_latitude: 48.85340071224621,
                expected_longitude: 2.348802387714386,
                score: 3663832752681684,
            },
            TestCase {
                name: "Sydney",
                expected_latitude: -33.86880091934156,
                expected_longitude: 151.2092998623848,
                score: 3252046221964352,
            },
            TestCase {
                name: "Tokyo",
                expected_latitude: 35.68950126697936,
                expected_longitude: 139.691701233387,
                score: 4171231230197045,
            },
            TestCase {
                name: "Vienna",
                expected_latitude: 48.20640046271915,
                expected_longitude: 16.370699107646942,
                score: 3673109836391743,
            },
        ];

        for test_case in test_cases {
            let result = decode(test_case.score);

            let lat_diff = (result.latitude - test_case.expected_latitude).abs();
            let lon_diff = (result.longitude - test_case.expected_longitude).abs();

            let success = lat_diff < 0.000001 && lon_diff < 0.000001;
            let status = if success { "✅" } else { "❌" };
            println!(
                "{}: (lat={:.15}, lon={:.15}) ({})",
                test_case.name, result.latitude, result.longitude, status
            );

            if !success {
                println!(
                    "  Expected: lat={:.15}, lon={:.15}",
                    test_case.expected_latitude, test_case.expected_longitude
                );
                println!("  Diff: lat={:.6}, lon={:.6}", lat_diff, lon_diff);
            }
        }
    }
}
