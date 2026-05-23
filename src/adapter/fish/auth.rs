use crate::error::{AppError, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Manages fish authentication: token acquisition, refresh, and cookie persistence.
pub struct AuthManager {
    pub client: reqwest::Client,
    pub cookies: Arc<Mutex<HashMap<String, String>>>,
    device_id: String,
}

impl AuthManager {
    pub fn new() -> Self {
        let device_id = crate::adapter::fish::sign::generate_device_id("");

        // Try to load existing cookies
        let cookies = Self::load_cookies_from_env();

        Self {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new(cookies)),
            device_id,
        }
    }

    /// Load cookies from environment variable (FISH_AUTH_JSON) or return empty.
    fn load_cookies_from_env() -> HashMap<String, String> {
        if let Ok(json_str) = std::env::var("FISH_AUTH_JSON") {
            if let Ok(map) = serde_json::from_str::<HashMap<String, String>>(&json_str) {
                tracing::info!("Loaded auth cookies from environment");
                return map;
            }
        }
        HashMap::new()
    }

    pub fn device_id(&self) -> String {
        self.device_id.clone()
    }

    pub async fn get_cookies(&self) -> HashMap<String, String> {
        self.cookies.lock().await.clone()
    }

    pub fn cookies_str_sync(&self) -> String {
        // For initial connection construction — uses synchronous lock if possible
        // In practice we build headers from the token, not cookies directly for WS
        String::new()
    }

    /// Build a cookie header string from current cookies.
    pub async fn cookie_header(&self) -> String {
        let cookies = self.cookies.lock().await;
        cookies
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Get the user's unb (user ID) from cookies.
    pub async fn my_id(&self) -> String {
        let cookies = self.cookies.lock().await;
        cookies.get("unb").cloned().unwrap_or_default()
    }

    /// Perform QR code login — returns the cookies dict.
    /// In the first iteration, this is a placeholder.
    pub async fn qrcode_login(&mut self) -> Result<HashMap<String, String>> {
        tracing::warn!(
            "QR code login not yet implemented. Set FISH_AUTH_JSON env var or implement QR flow."
        );
        Err(AppError::Auth("QR code login not implemented".into()))
    }

    /// Ensure we have valid auth, logging in if necessary.
    pub async fn from_local_or_qr_login() -> Result<Self> {
        let mut auth = Self::new();

        // If we have no cookies, try QR login
        {
            let cookies = auth.cookies.lock().await;
            if cookies.is_empty() {
                drop(cookies);
                auth.qrcode_login().await?;
            }
        }

        Ok(auth)
    }

    pub async fn refresh_if_needed(&mut self) -> Result<()> {
        // Token refresh — placeholder
        Ok(())
    }
}

impl Clone for AuthManager {
    fn clone(&self) -> Self {
        Self {
            client: reqwest::Client::new(),
            cookies: self.cookies.clone(),
            device_id: self.device_id.clone(),
        }
    }
}
