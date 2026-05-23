use crate::error::Result;

pub fn generate_sign(t: &str, token: &str, data: &str) -> Result<String> {
    // TODO: real FFI call to sign_core library
    Ok(format!("{}_{}_{}", t, token, data))
}

pub fn generate_mid() -> String {
    let uuid = uuid::Uuid::new_v4();
    uuid.to_string()
}

pub fn generate_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
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
