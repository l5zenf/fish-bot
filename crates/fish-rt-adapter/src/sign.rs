use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Number, Value};
use std::time::{SystemTime, UNIX_EPOCH};

use fish_core::error::Result;
use rand::Rng;

const APP_KEY: &str = "34839810";
const HEX: &[u8; 16] = b"0123456789ABCDEF";

/// MTOP h5 sign: md5(token&t&appKey&data).
pub(crate) fn generate_sign(t: &str, token: &str, data: &str) -> Result<String> {
    Ok(format!(
        "{:x}",
        md5::compute(format!("{token}&{t}&{APP_KEY}&{data}"))
    ))
}

/// Message ID for WebSocket frames: {rand_0..999}{timestamp_ms} 0
pub(crate) fn generate_mid() -> String {
    format!(
        "{}{} 0",
        rand::thread_rng().gen_range(0..1000u32),
        epoch_ms()
    )
}

/// UUID for WebSocket send: -{timestamp_ms}1
pub(crate) fn generate_uuid() -> String {
    format!("-{}1", epoch_ms())
}

/// Device fingerprint: UUIDv4-style (uppercase hex) with optional user-id suffix.
pub(crate) fn generate_device_id(user_id: &str) -> String {
    let mut rng = rand::thread_rng();
    let hex: String = (0..30).map(|_| HEX[rng.gen_range(0..16)] as char).collect();
    let variant = HEX[(rng.gen_range(0..4) | 0x8) as usize] as char;

    format!(
        "{}-{}-4{}-{}{}-{}-{user_id}",
        &hex[..8],
        &hex[8..12],
        &hex[12..15],
        variant,
        &hex[15..18],
        &hex[18..],
    )
}

pub(crate) fn clean_base64_input(raw: &str) -> String {
    let mut cleaned = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '='))
        .collect::<String>();

    while !cleaned.len().is_multiple_of(4) {
        cleaned.push('=');
    }

    cleaned
}

pub(crate) fn decode_python_like_payload(raw: &str) -> Result<Vec<u8>> {
    Ok(STANDARD.decode(clean_base64_input(raw).as_bytes())?)
}

pub(crate) fn decode_python_like_msgpack(bytes: &[u8]) -> Option<Value> {
    MessagePackDecoder::new(bytes).decode().ok()
}

struct MessagePackDecoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> MessagePackDecoder<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn decode(mut self) -> Result<Value> {
        self.decode_value()
    }

    fn decode_value(&mut self) -> Result<Value> {
        let format = self.read_byte()?;
        match format {
            0x00..=0x7f => Ok(Value::Number(Number::from(format))),
            0x80..=0x8f => self.decode_map((format & 0x0f) as usize),
            0x90..=0x9f => self.decode_array((format & 0x0f) as usize),
            0xa0..=0xbf => Ok(Value::String(self.read_string((format & 0x1f) as usize)?)),
            0xc0 => Ok(Value::Null),
            0xc2 => Ok(Value::Bool(false)),
            0xc3 => Ok(Value::Bool(true)),
            0xc4 => {
                let size = self.read_u8()? as usize;
                self.read_bin(size)
            }
            0xc5 => {
                let size = self.read_u16()? as usize;
                self.read_bin(size)
            }
            0xc6 => {
                let size = self.read_u32()? as usize;
                self.read_bin(size)
            }
            0xca => Ok(Value::Number(
                Number::from_f64(self.read_f32()? as f64)
                    .ok_or_else(|| fish_core::error::AppError::protocol("invalid float32"))?,
            )),
            0xcb => Ok(Value::Number(
                Number::from_f64(self.read_f64()?)
                    .ok_or_else(|| fish_core::error::AppError::protocol("invalid float64"))?,
            )),
            0xcc => Ok(Value::Number(Number::from(self.read_u8()?))),
            0xcd => Ok(Value::Number(Number::from(self.read_u16()?))),
            0xce => Ok(Value::Number(Number::from(self.read_u32()?))),
            0xcf => Ok(Value::Number(Number::from(self.read_u64()?))),
            0xd0 => Ok(Value::Number(Number::from(self.read_i8()?))),
            0xd1 => Ok(Value::Number(Number::from(self.read_i16()?))),
            0xd2 => Ok(Value::Number(Number::from(self.read_i32()?))),
            0xd3 => Ok(Value::Number(Number::from(self.read_i64()?))),
            0xd9 => {
                let size = self.read_u8()? as usize;
                Ok(Value::String(self.read_string(size)?))
            }
            0xda => {
                let size = self.read_u16()? as usize;
                Ok(Value::String(self.read_string(size)?))
            }
            0xdb => {
                let size = self.read_u32()? as usize;
                Ok(Value::String(self.read_string(size)?))
            }
            0xdc => {
                let size = self.read_u16()? as usize;
                self.decode_array(size)
            }
            0xdd => {
                let size = self.read_u32()? as usize;
                self.decode_array(size)
            }
            0xde => {
                let size = self.read_u16()? as usize;
                self.decode_map(size)
            }
            0xdf => {
                let size = self.read_u32()? as usize;
                self.decode_map(size)
            }
            0xe0..=0xff => Ok(Value::Number(Number::from((format as i8) as i64))),
            _ => Err(fish_core::error::AppError::protocol(format!(
                "unknown messagepack format byte: 0x{format:02x}"
            ))),
        }
    }

    fn decode_array(&mut self, size: usize) -> Result<Value> {
        let mut items = Vec::with_capacity(size);
        for _ in 0..size {
            items.push(self.decode_value()?);
        }
        Ok(Value::Array(items))
    }

    fn decode_map(&mut self, size: usize) -> Result<Value> {
        let mut map = serde_json::Map::with_capacity(size);
        for _ in 0..size {
            let key = self.decode_value()?;
            let value = self.decode_value()?;
            map.insert(Self::value_to_key(&key), value);
        }
        Ok(Value::Object(map))
    }

    fn value_to_key(value: &Value) -> String {
        match value {
            Value::String(text) => text.clone(),
            Value::Null => "null".to_string(),
            Value::Bool(flag) => flag.to_string(),
            Value::Number(number) => number.to_string(),
            other => other.to_string(),
        }
    }

    fn read_bin(&mut self, size: usize) -> Result<Value> {
        let bytes = self.read_bytes(size)?;
        match String::from_utf8(bytes.to_vec()) {
            Ok(text) => Ok(Value::String(text)),
            Err(_) => Ok(Value::String(STANDARD.encode(bytes))),
        }
    }

    fn read_string(&mut self, size: usize) -> Result<String> {
        let bytes = self.read_bytes(size)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|e| fish_core::error::AppError::protocol(format!("invalid utf-8 string: {e}")))
    }

    fn read_byte(&mut self) -> Result<u8> {
        let byte =
            self.data.get(self.pos).copied().ok_or_else(|| {
                fish_core::error::AppError::protocol("unexpected end of messagepack")
            })?;
        self.pos += 1;
        Ok(byte)
    }

    fn read_bytes(&mut self, count: usize) -> Result<&'a [u8]> {
        if self.pos + count > self.data.len() {
            return Err(fish_core::error::AppError::protocol(
                "unexpected end of messagepack buffer",
            ));
        }
        let slice = &self.data[self.pos..self.pos + count];
        self.pos += count;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u64> {
        Ok(self.read_byte()? as u64)
    }

    fn read_u16(&mut self) -> Result<u64> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]) as u64)
    }

    fn read_u32(&mut self) -> Result<u64> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as u64)
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_i8(&mut self) -> Result<i64> {
        Ok((self.read_byte()? as i8) as i64)
    }

    fn read_i16(&mut self) -> Result<i64> {
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]) as i64)
    }

    fn read_i32(&mut self) -> Result<i64> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64)
    }

    fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_f32(&mut self) -> Result<f32> {
        let bytes = self.read_bytes(4)?;
        Ok(f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_f64(&mut self) -> Result<f64> {
        let bytes = self.read_bytes(8)?;
        Ok(f64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

fn epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t3_19_generate_mid() {
        let mid = generate_mid();
        assert!(mid.ends_with(" 0"));
        let body = mid.strip_suffix(" 0").unwrap();
        assert!(body.len() >= 14); // 1-3 digit rand + 13 digit timestamp
    }

    #[test]
    fn t3_20_generate_uuid() {
        let uuid = generate_uuid();
        assert!(uuid.starts_with('-'));
        assert!(uuid.ends_with('1'));
        assert!(uuid.len() >= 15);
    }

    #[test]
    fn t3_21_generate_device_id() {
        let did = generate_device_id("user123");
        assert!(did.ends_with("-user123"));

        let base = did.strip_suffix("-user123").unwrap();
        assert_eq!(base.len(), 36);
        assert_eq!(base.matches('-').count(), 4);
        assert_eq!(base.chars().nth(14).unwrap(), '4'); // version
        let v = base.chars().nth(19).unwrap();
        assert!(matches!(v, '8' | '9' | 'A' | 'B')); // variant

        let did = generate_device_id("");
        assert_eq!(did.len(), 37);
        assert!(did.ends_with('-'));
    }

    #[test]
    fn t3_22_generate_sign() -> anyhow::Result<()> {
        let sig = generate_sign("1700000000000", "abc123", "{}")?;
        assert_eq!(sig, "0de974d31357bb908f6e33b5c404f0b9");
        Ok(())
    }

    #[test]
    fn t3_22b_generate_sign_cross_verify() -> anyhow::Result<()> {
        // Cross-verified against reference implementation
        assert_eq!(
            generate_sign("1700000000000", "abc123", "{}")?,
            "0de974d31357bb908f6e33b5c404f0b9"
        );
        assert_eq!(
            generate_sign("t", "token", "data")?,
            "4d63fc0d3ae96eb57b92cb12637e6269"
        );
        assert_eq!(
            generate_sign("", "", "")?,
            "4d04f571711c7683d0c5b45abfdc8ccb"
        );
        Ok(())
    }

    #[test]
    fn t3_23_clean_base64_input_strips_noise() {
        let cleaned = clean_base64_input("YWJj\n###");
        assert_eq!(cleaned, "YWJj");
    }

    #[test]
    fn t3_24_decode_python_like_msgpack_handles_bin_utf8() -> anyhow::Result<()> {
        let raw = vec![0x81, 0xa3, b'k', b'e', b'y', 0xc4, 0x03, b'a', b'b', b'c'];
        let decoded = decode_python_like_msgpack(&raw).expect("decode msgpack");
        assert_eq!(decoded["key"], "abc");
        Ok(())
    }

    #[test]
    fn t3_25_decode_python_like_msgpack_handles_bin_base64_fallback() -> anyhow::Result<()> {
        let raw = vec![0x81, 0xa3, b'k', b'e', b'y', 0xc4, 0x02, 0xff, 0x00];
        let decoded = decode_python_like_msgpack(&raw).expect("decode msgpack");
        assert_eq!(decoded["key"], STANDARD.encode([0xff, 0x00]));
        Ok(())
    }
}
