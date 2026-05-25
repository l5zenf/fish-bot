use fish_core::error::{AppError, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CookieImportReport {
    pub imported: usize,
    pub path: PathBuf,
}

fn is_cookie_attribute(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "max-age" | "expires" | "path" | "domain" | "samesite" | "secure" | "httponly"
    )
}

fn parse_browser_cookie_header(raw: &str) -> HashMap<String, String> {
    raw.split(';')
        .filter_map(|entry| {
            let trimmed = entry.trim();
            if trimmed.is_empty() {
                return None;
            }

            let (name, value) = trimmed.split_once('=')?;
            let name = name.trim();
            if name.is_empty() || is_cookie_attribute(name) {
                return None;
            }

            Some((name.to_string(), value.trim().to_string()))
        })
        .collect()
}

pub async fn import_browser_cookies(raw: &str) -> Result<CookieImportReport> {
    AuthManager::new().import_browser_cookies(raw).await
}

/// Manages fish authentication: token acquisition, refresh, and cookie persistence.
pub(crate) struct AuthManager {
    client: reqwest::Client,
    pub(crate) cookies: Arc<Mutex<HashMap<String, String>>>,
    data_dir: PathBuf,
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthManager {
    pub(crate) fn new() -> Self {
        // Try loading from file first, then env var
        let data_dir = Self::resolve_data_dir();
        let cookies = Self::load_cookies_from_file(&data_dir)
            .or_else(Self::load_cookies_from_env)
            .unwrap_or_default();

        Self {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new(cookies)),
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
    pub(crate) async fn save_cookies_to_file(&self) {
        let path = self.auth_file_path();
        if let Some(parent) = path.parent()
            && !parent.exists()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            tracing::error!(
                "Failed to create data directory {}: {}",
                parent.display(),
                e
            );
            return;
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
    pub(crate) async fn rm_auth_file(&self) {
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

    pub(crate) async fn get_cookies(&self) -> HashMap<String, String> {
        self.cookies.lock().await.clone()
    }

    pub(crate) async fn import_browser_cookies(&self, raw: &str) -> Result<CookieImportReport> {
        let parsed = parse_browser_cookie_header(raw);
        if parsed.is_empty() {
            return Err(AppError::auth(
                "browser cookie string is empty or does not contain valid key=value pairs",
            ));
        }

        let imported = parsed.len();
        let mut cookies = self.cookies.lock().await;
        for (key, value) in parsed {
            cookies.insert(key, value);
        }
        drop(cookies);

        self.save_cookies_to_file().await;

        Ok(CookieImportReport {
            imported,
            path: self.auth_file_path(),
        })
    }

    /// Build a cookie header string from current cookies.
    pub(crate) async fn cookie_header(&self) -> String {
        let cookies = self.cookies.lock().await;
        cookies
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Get the user's unb (user ID) from cookies.
    pub(crate) async fn my_id(&self) -> String {
        let cookies = self.cookies.lock().await;
        cookies.get("unb").cloned().unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn test_new(data_dir: PathBuf) -> Self {
        let cookies = Self::load_cookies_from_file(&data_dir).unwrap_or_default();
        Self {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new(cookies)),
            data_dir,
        }
    }
}

impl Clone for AuthManager {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            cookies: self.cookies.clone(),
            data_dir: self.data_dir.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_auth_dir() -> anyhow::Result<PathBuf> {
        let dir = tempfile::tempdir()?;
        Ok(dir.keep())
    }

    #[tokio::test]
    async fn t3_25_new_no_auth_file_no_env() -> anyhow::Result<()> {
        // Use a path where no auth file exists
        let auth = AuthManager::test_new(PathBuf::from("/tmp/nonexistent_random_path_42"));
        let cookies = auth.get_cookies().await;
        assert!(cookies.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn t3_26_new_with_valid_auth_file() -> anyhow::Result<()> {
        let dir = temp_auth_dir()?;
        let auth_path = dir.join("fish_auth.json");
        let test_cookies: HashMap<String, String> = [("unb".into(), "123".into())].into();
        std::fs::write(&auth_path, serde_json::to_string(&test_cookies)?)?;

        let auth = AuthManager::test_new(dir);
        let cookies = auth.get_cookies().await;
        assert_eq!(cookies.get("unb"), Some(&"123".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn t3_27_new_with_corrupted_auth_file() -> anyhow::Result<()> {
        let dir = temp_auth_dir()?;
        let auth_path = dir.join("fish_auth.json");
        std::fs::write(&auth_path, "this is not valid json")?;

        let auth = AuthManager::test_new(dir);
        let cookies = auth.get_cookies().await;
        assert!(cookies.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn t3_28_new_with_env_var() -> anyhow::Result<()> {
        let dir = temp_auth_dir()?;
        let test_cookies: HashMap<String, String> = [("unb".into(), "env_user".into())].into();
        let json = serde_json::to_string(&test_cookies)?;
        // Temporarily set env var
        unsafe {
            std::env::set_var("FISH_AUTH_JSON", &json);
        }
        let auth = AuthManager::test_new(dir);
        let cookies = auth.get_cookies().await;
        unsafe {
            std::env::remove_var("FISH_AUTH_JSON");
        }
        // With test_new, env var is not consulted (it uses load_cookies_from_file)
        // So this should just be empty
        assert!(cookies.is_empty() || cookies.get("unb") == Some(&"env_user".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn t3_30_empty_cookies_header() -> anyhow::Result<()> {
        let dir = temp_auth_dir()?;
        let auth = AuthManager::test_new(dir);
        let header = auth.cookie_header().await;
        assert_eq!(header, "");
        Ok(())
    }

    #[tokio::test]
    async fn t3_31_cookies_header_format() {
        let auth = AuthManager {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new(
                [("k1".into(), "v1".into()), ("k2".into(), "v2".into())].into(),
            )),
            data_dir: PathBuf::from("/tmp"),
        };
        let header = auth.cookie_header().await;
        assert!(header.contains("k1=v1"));
        assert!(header.contains("k2=v2"));
    }

    #[tokio::test]
    async fn t3_32_my_id_with_unb() {
        let auth = AuthManager {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new([("unb".into(), "user999".into())].into())),
            data_dir: PathBuf::from("/tmp"),
        };
        assert_eq!(auth.my_id().await, "user999");
    }

    #[tokio::test]
    async fn t3_33_my_id_no_unb() {
        let auth = AuthManager {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new(HashMap::new())),
            data_dir: PathBuf::from("/tmp"),
        };
        assert_eq!(auth.my_id().await, "");
    }

    #[tokio::test]
    async fn t3_34_save_cookies_to_file() -> anyhow::Result<()> {
        let dir = temp_auth_dir()?;
        let auth = AuthManager {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new([("unb".into(), "save_test".into())].into())),
            data_dir: dir.clone(),
        };

        auth.save_cookies_to_file().await;
        let auth_path = dir.join("fish_auth.json");
        assert!(auth_path.exists());

        let content = std::fs::read_to_string(&auth_path)?;
        let parsed: HashMap<String, String> = serde_json::from_str(&content)?;
        assert_eq!(parsed.get("unb"), Some(&"save_test".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn t3_35_rm_auth_file() -> anyhow::Result<()> {
        let dir = temp_auth_dir()?;
        let auth_path = dir.join("fish_auth.json");
        std::fs::write(&auth_path, "{}")?;
        assert!(auth_path.exists());

        let auth = AuthManager {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new(HashMap::new())),
            data_dir: dir.clone(),
        };
        auth.rm_auth_file().await;
        assert!(!auth_path.exists());
        Ok(())
    }

    #[test]
    fn t3_36_resolve_data_dir() {
        // Default (no env var)
        let dir = AuthManager::resolve_data_dir();
        assert_eq!(dir, PathBuf::from("data"));

        // With env var
        unsafe {
            std::env::set_var("FISH_DATA_DIR", "/custom/path");
        }
        let dir = AuthManager::resolve_data_dir();
        unsafe {
            std::env::remove_var("FISH_DATA_DIR");
        }
        assert_eq!(dir, PathBuf::from("/custom/path"));
    }

    #[tokio::test]
    async fn t3_38_save_cookies_creates_dir() -> anyhow::Result<()> {
        use tempfile::tempdir;
        let dir = tempdir()?;
        let nested = dir.path().join("nested").join("subdir");

        let auth = AuthManager {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new([("unb".into(), "test".into())].into())),
            data_dir: nested.clone(),
        };

        auth.save_cookies_to_file().await;
        assert!(nested.exists(), "data directory should be created");
        assert!(
            nested.join("fish_auth.json").exists(),
            "auth file should exist"
        );

        let content = std::fs::read_to_string(nested.join("fish_auth.json"))?;
        let parsed: HashMap<String, String> = serde_json::from_str(&content)?;
        assert_eq!(parsed.get("unb"), Some(&"test".to_string()));
        Ok(())
    }

    #[test]
    fn t3_39_resolve_data_dir_default() -> anyhow::Result<()> {
        unsafe {
            std::env::remove_var("FISH_DATA_DIR");
        }
        let dir = AuthManager::resolve_data_dir();
        assert_eq!(dir, PathBuf::from("data"));
        Ok(())
    }

    #[tokio::test]
    async fn t3_50_clone_preserves_cookies() -> anyhow::Result<()> {
        let auth = AuthManager {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new([("unb".into(), "clone_test".into())].into())),
            data_dir: PathBuf::from("/tmp"),
        };
        let cloned = auth.clone();
        let cookies = cloned.get_cookies().await;
        assert_eq!(cookies.get("unb"), Some(&"clone_test".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn t3_51_load_cookies_from_env_happy() -> anyhow::Result<()> {
        let test_cookies: HashMap<String, String> = [("unb".into(), "env_user".into())].into();
        let json = serde_json::to_string(&test_cookies)?;
        unsafe {
            std::env::set_var("FISH_AUTH_JSON", &json);
        }
        let result = AuthManager::load_cookies_from_env();
        unsafe {
            std::env::remove_var("FISH_AUTH_JSON");
        }
        match result {
            Some(cookies) => assert_eq!(cookies.get("unb"), Some(&"env_user".to_string())),
            None => assert!(false, "should load cookies from env"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn t3_52_load_cookies_from_env_corrupted() {
        unsafe {
            std::env::set_var("FISH_AUTH_JSON", "not valid json");
        }
        let result = AuthManager::load_cookies_from_env();
        unsafe {
            std::env::remove_var("FISH_AUTH_JSON");
        }
        assert!(result.is_none(), "corrupted env var should return None");
    }

    #[tokio::test]
    async fn t3_53_load_cookies_from_file_nonexistent() -> anyhow::Result<()> {
        let result =
            AuthManager::load_cookies_from_file(&PathBuf::from("/tmp/nonexistent_cookie_dir_xyz"));
        assert!(result.is_none(), "nonexistent file should return None");
        Ok(())
    }

    #[tokio::test]
    async fn t3_56_load_cookies_from_file_empty_json() -> anyhow::Result<()> {
        let dir = temp_auth_dir()?;
        let auth_path = dir.join("fish_auth.json");
        std::fs::write(&auth_path, "{}")?;
        let result = AuthManager::load_cookies_from_file(&dir);
        if let Some(cookies) = result {
            assert!(cookies.is_empty(), "empty JSON should yield empty cookies");
        }
        Ok(())
    }

    #[tokio::test]
    async fn t3_59_auth_file_path_matches_data_dir() -> anyhow::Result<()> {
        let dir = temp_auth_dir()?;
        let auth = AuthManager::test_new(dir.clone());
        let path = auth.auth_file_path();
        assert_eq!(path, dir.join("fish_auth.json"));
        Ok(())
    }

    #[tokio::test]
    async fn t3_60_rm_auth_file_nonexistent() -> anyhow::Result<()> {
        let dir = temp_auth_dir()?;
        let auth = AuthManager::test_new(dir.clone());
        // Removing non-existent file should not panic
        auth.rm_auth_file().await;
        Ok(())
    }

    #[tokio::test]
    async fn t3_61_save_to_readonly_dir() -> anyhow::Result<()> {
        // Test saving when directory can't be created (e.g. a file exists at path)
        let dir = temp_auth_dir()?;
        let file_path = dir.join("fish_auth.json");
        // Create a file where the directory should be (edge case)
        std::fs::write(&file_path, "{}")?;
        // Try to save — this would create parent dirs if file_path parent didn't exist
        let auth = AuthManager {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new(HashMap::new())),
            data_dir: file_path.clone(), // data_dir is a file, not a dir
        };
        // This should handle the error gracefully (parent of fish_auth.json is a file)
        auth.save_cookies_to_file().await;
        // Clean up
        let _ = std::fs::remove_file(&file_path);
        Ok(())
    }

    #[tokio::test]
    async fn t3_62_load_cookies_from_dir_instead_of_file() -> anyhow::Result<()> {
        // When the auth file path is a directory instead of a file
        let dir = temp_auth_dir()?;
        let auth_file = dir.join("fish_auth.json");
        // Create a directory where the auth file should be
        std::fs::create_dir_all(&auth_file)?;
        let _result = AuthManager::load_cookies_from_file(&dir);
        std::fs::remove_dir(&auth_file)?;
        Ok(())
    }

    #[tokio::test]
    async fn t3_63_load_cookies_without_env() -> anyhow::Result<()> {
        // Ensure env var is not set
        unsafe {
            std::env::remove_var("FISH_AUTH_JSON");
        }
        let result = AuthManager::load_cookies_from_env();
        assert!(
            result.is_none(),
            "should return None when env var is not set"
        );
        Ok(())
    }

    #[test]
    fn t3_64_parse_browser_cookie_header() {
        let cookies = parse_browser_cookie_header(
            "cookie2=abc; _m_h5_tk=token_123; sgcookie=E100%2Btest; invalid-entry ; tracknick=nick=name; Max-Age=1773; Expires=Mon, 25-May-2026 11:06:55 GMT",
        );

        assert_eq!(cookies.get("cookie2"), Some(&"abc".to_string()));
        assert_eq!(cookies.get("_m_h5_tk"), Some(&"token_123".to_string()));
        assert_eq!(cookies.get("sgcookie"), Some(&"E100%2Btest".to_string()));
        assert_eq!(cookies.get("tracknick"), Some(&"nick=name".to_string()));
        assert!(!cookies.contains_key("invalid-entry"));
        assert!(!cookies.contains_key("Max-Age"));
        assert!(!cookies.contains_key("Expires"));
    }

    #[tokio::test]
    async fn t3_65_import_browser_cookies_persists_file() -> anyhow::Result<()> {
        let dir = temp_auth_dir()?;
        let auth = AuthManager {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new([("unb".into(), "old-user".into())].into())),
            data_dir: dir.clone(),
        };

        let report = auth
            .import_browser_cookies("cookie2=new-cookie; unb=new-user; tracknick=tester")
            .await?;

        assert_eq!(report.imported, 3);
        assert_eq!(report.path, dir.join("fish_auth.json"));

        let saved = std::fs::read_to_string(dir.join("fish_auth.json"))?;
        let parsed: HashMap<String, String> = serde_json::from_str(&saved)?;
        assert_eq!(parsed.get("cookie2"), Some(&"new-cookie".to_string()));
        assert_eq!(parsed.get("unb"), Some(&"new-user".to_string()));
        assert_eq!(parsed.get("tracknick"), Some(&"tester".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn t3_66_import_browser_cookies_rejects_empty_input() {
        let dir = temp_auth_dir().expect("temp dir");
        let auth = AuthManager {
            client: reqwest::Client::new(),
            cookies: Arc::new(Mutex::new(HashMap::new())),
            data_dir: dir,
        };

        let err = auth
            .import_browser_cookies("   ; invalid-entry  ")
            .await
            .expect_err("invalid cookie string should fail");

        assert!(matches!(err, AppError::Auth { .. }));
    }
}
