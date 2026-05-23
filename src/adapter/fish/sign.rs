use crate::error::Result;

pub fn generate_sign(t: &str, token: &str, data: &str) -> Result<String> {
    // TODO: real FFI call to sign_core library
    Ok(format!("{}_{}_{}", t, token, data))
}

pub fn generate_mid() -> String {
    // TODO: real FFI call
    "mock_mid".to_string()
}

pub fn generate_uuid() -> String {
    // TODO: real FFI call
    use std::time::{SystemTime, UNIX_EPOCH};
    format!("uuid-{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs())
}

pub fn generate_device_id(user_id: &str) -> String {
    // TODO: real FFI call
    format!("device_{}", user_id)
}

pub fn decrypt(b64_str: &str) -> Result<serde_json::Value> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let decoded = STANDARD.decode(b64_str)?;
    let json_str = String::from_utf8_lossy(&decoded);
    Ok(serde_json::from_str(&json_str)?)
}
