use fish_core::error::{AppError, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Manages fish authentication: token acquisition, refresh, and cookie persistence.
pub struct AuthManager {
    pub client: reqwest::Client,
    pub cookies: Arc<Mutex<HashMap<String, String>>>,
    device_id: String,
    data_dir: PathBuf,
}

impl AuthManager {
    pub fn new() -> Self {
        let device_id = crate::fish::sign::generate_device_id("");

        // Try loading from file first, then env var
        let data_dir = Self::resolve_data_dir();
        let cookies = Self::load_cookies_from_file(&data_dir)
            .or_else(|| Self::load_cookies_from_env())
            .unwrap_or_default();

        Self {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new(cookies)),
            device_id,
            data_dir,
        }
    }

    /// Resolve the data directory from FISH_DATA_DIR env var or default to ./data.
    fn resolve_data_dir() -> PathBuf {
        if let Ok(dir) = std::env::var("FISH_DATA_DIR") {
            PathBuf::from(dir)
        } else {
            PathBuf::from("data")
        }
    }

    /// Path to the auth cookie file.
    fn auth_file_path(&self) -> PathBuf {
        self.data_dir.join("fish_auth.json")
    }

    /// Load cookies from the local auth file.
    fn load_cookies_from_file(data_dir: &PathBuf) -> Option<HashMap<String, String>> {
        let path = data_dir.join("fish_auth.json");
        if !path.exists() {
            return None;
        }
        match std::fs::read_to_string(&path) {
            Ok(content) => match serde_json::from_str::<HashMap<String, String>>(&content) {
                Ok(map) => {
                    tracing::info!("Loaded auth cookies from {}", path.display());
                    Some(map)
                }
                Err(_) => {
                    tracing::warn!("Auth file {} is corrupted, ignoring", path.display());
                    None
                }
            },
            Err(e) => {
                tracing::warn!("Failed to read auth file {}: {}", path.display(), e);
                None
            }
        }
    }

    /// Save cookies to the local auth file.
    pub async fn save_cookies_to_file(&self) {
        let path = self.auth_file_path();
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    tracing::error!("Failed to create data directory {}: {}", parent.display(), e);
                    return;
                }
            }
        }
        let cookies = self.cookies.lock().await;
        match serde_json::to_string_pretty(&*cookies) {
            Ok(json) => match std::fs::write(&path, &json) {
                Ok(_) => tracing::info!("Auth cookies saved to {}", path.display()),
                Err(e) => tracing::error!("Failed to save auth cookies: {}", e),
            },
            Err(e) => tracing::error!("Failed to serialize auth cookies: {}", e),
        }
    }

    /// Remove the local auth file.
    pub async fn rm_auth_file(&self) {
        let path = self.auth_file_path();
        if path.exists() {
            if let Err(e) = std::fs::remove_file(&path) {
                tracing::error!("Failed to remove auth file {}: {}", path.display(), e);
            } else {
                tracing::info!("Removed auth file {}", path.display());
            }
        }
    }

    /// Load cookies from environment variable (FISH_AUTH_JSON).
    fn load_cookies_from_env() -> Option<HashMap<String, String>> {
        if let Ok(json_str) = std::env::var("FISH_AUTH_JSON") {
            match serde_json::from_str::<HashMap<String, String>>(&json_str) {
                Ok(map) => {
                    tracing::info!("Loaded auth cookies from environment");
                    Some(map)
                }
                Err(_) => None,
            }
        } else {
            None
        }
    }

    pub fn device_id(&self) -> String {
        self.device_id.clone()
    }

    pub async fn get_cookies(&self) -> HashMap<String, String> {
        self.cookies.lock().await.clone()
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
    pub async fn qrcode_login(&mut self) -> Result<HashMap<String, String>> {
        // QR login is now orchestrated at the adapter level
        Err(AppError::Auth("QR code login not implemented".into()))
    }

    /// Ensure we have valid auth, logging in if necessary.
    pub async fn from_local_or_qr_login() -> Result<Self> {
        let auth = Self::new();

        // If we have no cookies, try QR login
        {
            let cookies = auth.cookies.lock().await;
            if cookies.is_empty() {
                // Will drop lock before QR login
            }
        }

        Ok(auth)
    }

    pub async fn refresh_if_needed(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Clone for AuthManager {
    fn clone(&self) -> Self {
        Self {
            client: reqwest::Client::new(),
            cookies: self.cookies.clone(),
            device_id: self.device_id.clone(),
            data_dir: self.data_dir.clone(),
        }
    }
}
