use fish_core::error::Result;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t3_19_generate_mid() {
        let mid = generate_mid();
        assert!(!mid.is_empty());
        // UUID format: 8-4-4-4-12 hex digits
        assert_eq!(mid.len(), 36);
        assert_eq!(mid.chars().filter(|&c| c == '-').count(), 4);
    }

    #[test]
    fn t3_20_generate_uuid() {
        let uuid = generate_uuid();
        assert!(!uuid.is_empty());
        assert_eq!(uuid.len(), 36);
    }

    #[test]
    fn t3_21_generate_device_id() {
        let did = generate_device_id("user123");
        assert!(did.contains("user123"));
    }

    #[test]
    fn t3_22_generate_sign() -> anyhow::Result<()> {
        let sig = generate_sign("t", "token", "data")?;
        assert!(!sig.is_empty());
        assert_eq!(sig, "t_token_data");
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
        let result = decrypt("!!!invalid-base64!!!");
        assert!(result.is_err());
    }
}
