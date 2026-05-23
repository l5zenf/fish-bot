use crate::error::{AppError, Result};
use crate::adapter::fish::auth::AuthManager;
use crate::adapter::fish::sign::{generate_sign, generate_device_id};
use serde_json::Value;
use std::collections::HashMap;

/// Fish API client wrapping the MTOP (Mobile Taobao Open Platform) protocol.
pub struct FishAPI {
    client: reqwest::Client,
    auth: AuthManager,
    device_id: String,
}

impl FishAPI {
    pub fn new(auth: AuthManager) -> Self {
        let device_id = generate_device_id("");
        Self {
            client: reqwest::Client::builder()
                .cookie_store(true)
                .default_headers({
                    let mut headers = reqwest::header::HeaderMap::new();
                    headers.insert(
                        reqwest::header::ACCEPT,
                        "application/json".parse().unwrap(),
                    );
                    headers.insert(
                        reqwest::header::CONTENT_TYPE,
                        "application/x-www-form-urlencoded".parse().unwrap(),
                    );
                    headers.insert(
                        reqwest::header::ORIGIN,
                        "https://www.goofish.com".parse().unwrap(),
                    );
                    headers.insert(
                        reqwest::header::REFERER,
                        "https://www.goofish.com/".parse().unwrap(),
                    );
                    headers
                })
                .build()
                .unwrap_or_default(),
            auth,
            device_id,
        }
    }

    pub fn device_id(&self) -> String {
        self.device_id.clone()
    }

    pub async fn cookies_str(&self) -> String {
        self.auth.cookie_header().await
    }

    pub async fn my_id(&self) -> String {
        self.auth.my_id().await
    }

    // ---- Core MTOP call ----

    /// Make an MTOP API call with full sign and cookie handling.
    pub async fn call_mtop(
        &self,
        api: &str,
        version: &str,
        data: &Value,
        extra_params: Option<&HashMap<String, String>>,
    ) -> Result<Value> {
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let t_str = t.to_string();
        let data_val = if data.is_null() || data.as_object().map_or(true, |o| o.is_empty()) {
            "{}".to_string()
        } else {
            serde_json::to_string(data)?
        };

        // Get token from cookies (stored via reqwest cookie jar)
        // We need manual cookie management since reqwest::Client stores cookies internally
        let cookies = self.auth.get_cookies().await;
        let tk = cookies.get("_m_h5_tk").cloned().unwrap_or_default();
        let token = tk.split('_').next().unwrap_or("").to_string();

        // Sign the request
        let sign = if !token.is_empty() && !data_val.is_empty() {
            generate_sign(&t_str, &token, &data_val).unwrap_or_default()
        } else {
            String::new()
        };

        let mut params = HashMap::new();
        params.insert("jsv", "2.7.2");
        params.insert("appKey", "34839810");
        params.insert("t", &t_str);
        params.insert("sign", &sign);
        params.insert("v", version);
        params.insert("type", "originaljson");
        params.insert("dataType", "json");
        params.insert("api", api);
        params.insert("timeout", "20000");
        params.insert("sessionOption", "AutoLoginOnly");

        if let Some(extra) = extra_params {
            for (k, v) in extra {
                params.insert(k, v);
            }
        }

        let url = format!(
            "https://h5api.m.goofish.com/h5/{}/{}",
            api.to_lowercase(),
            version
        );

        let body = format!("data={}", urlencoding(&data_val));

        tracing::debug!("MTOP call: {}?{:?}", url, params);

        let response = self
            .client
            .post(&url)
            .query(&params)
            .header("cookie", self.auth.cookie_header().await)
            .body(body)
            .send()
            .await?;

        // Update cookies from response headers
        if let Some(set_cookie) = response.headers().get("set-cookie") {
            if let Ok(cookie_str) = set_cookie.to_str() {
                for part in cookie_str.split(';') {
                    if let Some(eq) = part.find('=') {
                        let k = part[..eq].trim().to_string();
                        let v = part[eq + 1..].trim().to_string();
                        let mut cookies: tokio::sync::MutexGuard<'_, std::collections::HashMap<std::string::String, std::string::String>> = self.auth.cookies.lock().await;
                        cookies.insert(k, v);
                    }
                }
            }
        }

        let json: Value = response.json().await?;

        // Check for error ret
        if let Some(ret) = json.get("ret").and_then(|v| v.as_array()) {
            if ret.iter().all(|r| !r.as_str().map_or(false, |s| s.contains("SUCCESS"))) {
                tracing::warn!("MTOP API error: {:?}", ret);
            }
        }

        Ok(json)
    }

    // ---- Auth APIs ----

    /// Get token (first step of access token acquisition).
    pub async fn get_token(&self) -> Result<Value> {
        let data = serde_json::json!({
            "appKey": "444e9908a51d1cb236a27862abc769c9",
            "deviceId": self.device_id,
        });
        let mut extra = HashMap::new();
        extra.insert("spm_pre".to_string(), "a21ybx.item.want.1.14ad3da6ALVq3n".to_string());
        extra.insert("log_id".to_string(), "14ad3da6ALVq3n".to_string());
        self.call_mtop(
            "mtop.taobao.idlemessage.pc.login.token",
            "1.0",
            &data,
            Some(&extra),
        )
        .await
    }

    pub async fn get_access_token(&self) -> Result<String> {
        let res = self.get_token().await?;
        res.get("data")
            .and_then(|d| d.get("accessToken"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| AppError::Auth("No accessToken in response".into()))
    }

    pub async fn get_mh5tk(&self) -> Result<Value> {
        self.call_mtop(
            "mtop.gaia.nodejs.gaia.idle.data.gw.v2.index.get",
            "1.0",
            &serde_json::json!({}),
            None,
        )
        .await
    }

    // ---- User APIs ----

    pub async fn get_user_info(&self, user_id: &str) -> Result<Value> {
        let data = serde_json::json!({
            "self": user_id.is_empty(),
            "userId": user_id,
        });
        let mut extra = HashMap::new();
        extra.insert("spm_pre".to_string(), "a21ybx.home.nav.1.62953da6OYFsax".to_string());
        extra.insert("log_id".to_string(), "62953da6OYFsax".to_string());
        self.call_mtop("mtop.idle.web.user.page.head", "1.0", &data, Some(&extra))
            .await
    }

    // ---- Item APIs ----

    pub async fn get_item_list(&self, user_id: &str, page: u64, page_size: u64) -> Result<Value> {
        let data = serde_json::json!({
            "userId": user_id,
            "pageNumber": page,
            "pageSize": page_size,
        });
        let mut extra = HashMap::new();
        extra.insert("spm_pre".to_string(), "a21ybx.home.nav.1.62953da6OYFsax".to_string());
        extra.insert("log_id".to_string(), "62953da6OYFsax".to_string());
        self.call_mtop("mtop.idle.web.xyh.item.list", "1.0", &data, Some(&extra))
            .await
    }

    pub async fn get_item_info(&self, item_id: &str) -> Result<Value> {
        let data = serde_json::json!({ "itemId": item_id });
        let mut extra = HashMap::new();
        extra.insert("spm_pre".to_string(), "a21ybx.item.want.1.12523da6waCtUp".to_string());
        extra.insert("log_id".to_string(), "12523da6waCtUp".to_string());
        self.call_mtop("mtop.taobao.idle.pc.detail", "1.0", &data, Some(&extra))
            .await
    }
}

impl Clone for FishAPI {
    fn clone(&self) -> Self {
        Self {
            client: reqwest::Client::new(),
            auth: self.auth.clone(),
            device_id: self.device_id.clone(),
        }
    }
}

/// Simple URL encoder for form bodies.
fn urlencoding(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char);
            }
            b' ' => result.push_str("%20"),
            _ => {
                result.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    result
}
