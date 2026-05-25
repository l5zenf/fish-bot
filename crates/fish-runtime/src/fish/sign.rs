use std::time::{SystemTime, UNIX_EPOCH};

use fish_core::error::Result;
use rand::Rng;

const APP_KEY: &str = "34839810";
const HEX: &[u8; 16] = b"0123456789ABCDEF";

/// MTOP h5 sign: md5(token&t&appKey&data).
pub fn generate_sign(t: &str, token: &str, data: &str) -> Result<String> {
    Ok(format!(
        "{:x}",
        md5::compute(format!("{token}&{t}&{APP_KEY}&{data}"))
    ))
}

/// Message ID for WebSocket frames: {rand_0..999}{timestamp_ms} 0
pub fn generate_mid() -> String {
    format!(
        "{}{} 0",
        rand::thread_rng().gen_range(0..1000u32),
        epoch_ms()
    )
}

/// UUID for WebSocket send: -{timestamp_ms}1
pub fn generate_uuid() -> String {
    format!("-{}1", epoch_ms())
}

/// Device fingerprint: UUIDv4-style (uppercase hex) with optional user-id suffix.
pub fn generate_device_id(user_id: &str) -> String {
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

/// Decrypt a base64-encoded message body to JSON.
pub fn decrypt(b64_str: &str) -> Result<serde_json::Value> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let decoded = STANDARD.decode(b64_str)?;
    Ok(serde_json::from_slice(&decoded)?)
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
    fn t3_23_decrypt_valid() -> anyhow::Result<()> {
        let payload = serde_json::json!({"key": "value"});
        let b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            serde_json::to_string(&payload)?,
        );
        let result = decrypt(&b64)?;
        assert_eq!(result["key"], "value");
        Ok(())
    }

    #[test]
    fn t3_24_decrypt_invalid_base64() {
        assert!(decrypt("!!!invalid-base64!!!").is_err());
    }
}
