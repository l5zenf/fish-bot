use crate::error::{AppError, Result};
use crate::adapter::fish::auth::AuthManager;
use crate::adapter::BaseAPI;
use crate::adapter::fish::sign::{generate_sign, generate_device_id};
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::Mutex;

/// Fish API client wrapping the MTOP (Mobile Taobao Open Platform) protocol.
pub struct FishAPI {
    client: reqwest::Client,
    auth: AuthManager,
    device_id: String,
    /// Login form params for QR code flow, shared between gen and poll.
    pub poll_params: Mutex<Option<HashMap<String, String>>>,
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
                    headers.insert(
                        reqwest::header::USER_AGENT,
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0".parse().unwrap(),
                    );
                    headers.insert(
                        reqwest::header::ACCEPT_LANGUAGE,
                        "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6".parse().unwrap(),
                    );
                    headers
                })
                .build()
                .unwrap_or_default(),
            auth,
            device_id,
            poll_params: Mutex::new(None),
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

    pub fn auth(&self) -> &AuthManager {
        &self.auth
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

        // Get token from cookies
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
        self.save_cookies_from_response(&response).await;

        let json: Value = response.json().await?;

        // Check for error ret
        if let Some(ret) = json.get("ret").and_then(|v| v.as_array()) {
            if ret.iter().all(|r| !r.as_str().map_or(false, |s| s.contains("SUCCESS"))) {
                tracing::warn!("MTOP API error: {:?}", ret);
            }
        }

        Ok(json)
    }

    /// Extract and persist cookies from a response's Set-Cookie headers.
    pub async fn save_cookies_from_response(&self, response: &reqwest::Response) {
        let mut cookie_updated = false;
        let mut cookies = self.auth.cookies.lock().await;

        for cookie_header in response.headers().get_all("set-cookie") {
            if let Ok(cookie_str) = cookie_header.to_str() {
                let pure = cookie_str.split(';').next().unwrap_or("").trim().to_string();
                if let Some(eq) = pure.find('=') {
                    let k = pure[..eq].trim().to_string();
                    let v = pure[eq + 1..].trim().to_string();
                    if cookies.get(&k).map(|s| s.as_str()) != Some(&v) {
                        cookies.insert(k, v);
                        cookie_updated = true;
                    }
                }
            }
        }
        drop(cookies);

        if cookie_updated {
            self.auth.save_cookies_to_file().await;
        }
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

    // ---- QR Code Login ----

    /// Fetch login form params from passport.goofish.com.
    /// Parses window.viewData from the mini_login.htm page.
    /// Returns loginFormData (used as query params for QR generation).
    /// Also stores the initial params (lang, appName...) in poll_params
    /// for later use in qrcode_poll (POST form data).
    async fn _get_login_params(&self) -> Result<HashMap<String, String>> {
        let rnd: f64 = rand::random();

        let initial_params: HashMap<String, String> = [
            ("lang", "zh_cn"),
            ("appName", "xianyu"),
            ("appEntrance", "web"),
            ("styleType", "vertical"),
            ("bizParams", ""),
            ("notLoadSsoView", "False"),
            ("notKeepLogin", "False"),
            ("isMobile", "False"),
            ("qrCodeFirst", "False"),
            ("stie", "77"),
            ("rnd", &rnd.to_string()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        // Store initial params in poll_params for later use in qrcode_poll
        {
            let mut poll_guard = self.poll_params.lock().await;
            *poll_guard = Some(initial_params.clone());
        }

        let url = "https://passport.goofish.com/mini_login.htm";
        let resp = self
            .client
            .get(url)
            .query(&initial_params)
            .send()
            .await?;

        let html = resp.text().await?;

        // Parse window.viewData = {...}; from HTML.
        // The viewData JSON may span multiple lines and contain nested objects,
        // so we find the start marker and use a simple brace-counter approach.
        let mut login_params = match html.find("window.viewData") {
            Some(idx) => {
                let start = html[idx..].find('{');
                match start {
                    Some(brace_idx) => {
                        let json_start = idx + brace_idx;
                        let mut depth = 0;
                        let mut end = json_start;
                        for (i, ch) in html[json_start..].char_indices() {
                            if ch == '{' { depth += 1; }
                            else if ch == '}' { depth -= 1; }
                            if depth == 0 {
                                end = json_start + i + 1;
                                break;
                            }
                        }
                        let json_str = &html[json_start..end];
                        match serde_json::from_str::<serde_json::Value>(json_str) {
                            Ok(view_data) => {
                                // Extract loginFormData which is the nested form fields object
                                view_data
                                    .get("loginFormData")
                                    .and_then(|v| v.as_object())
                                    .map(|obj| {
                                        obj.iter()
                                            .map(|(k, v)| {
                                                let v_str = match v {
                                                    serde_json::Value::String(s) => s.clone(),
                                                    serde_json::Value::Bool(b) => b.to_string(),
                                                    serde_json::Value::Number(n) => n.to_string(),
                                                    serde_json::Value::Null => String::new(),
                                                    // For arrays/objects, serialize back
                                                    other => other.to_string(),
                                                };
                                                (k.clone(), v_str)
                                            })
                                            .collect::<HashMap<String, String>>()
                                    })
                                    .unwrap_or_default()
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to parse viewData JSON: {}, falling back to empty params",
                                    e
                                );
                                HashMap::new()
                            }
                        }
                    }
                    None => {
                        tracing::warn!("Could not find brace in viewData");
                        HashMap::new()
                    }
                }
            }
            None => {
                tracing::warn!("Could not find window.viewData in mini_login.htm response");
                HashMap::new()
            }
        };

        login_params.insert("umidTag".to_string(), "SERVER".to_string());

        Ok(login_params)
    }

    /// Generate a QR code for login. Returns t, ck, and codeContent URL.
    pub async fn qrcode_gen(&self) -> Result<Option<HashMap<String, String>>> {
        let params = self._get_login_params().await?;
        if params.is_empty() {
            tracing::error!("Failed to get login params, cannot generate QR code");
            return Ok(None);
        }

        let url = "https://passport.goofish.com/newlogin/qrcode/generate.do";
        let resp = self.client.get(url).query(&params).send().await?;

        let body: Value = resp.json().await?;

        let success = body
            .get("content")
            .and_then(|c| c.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if !success {
            tracing::error!("QR code generation failed: {:?}", body);
            return Ok(None);
        }

        let data = body
            .get("content")
            .and_then(|c| c.get("data"))
            .and_then(|d| d.as_object())
            .cloned()
            .unwrap_or_default();

        tracing::debug!("qrcode_gen response data: {:?}", data);

        // Helper to extract a value as string regardless of JSON type
        let as_string = |v: &serde_json::Value| -> String {
            match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                _ => String::new(),
            }
        };

        let t = data.get("t").map(|v| as_string(v)).unwrap_or_default();
        let ck = data.get("ck").map(|v| as_string(v)).unwrap_or_default();
        let code_content = data.get("codeContent").map(|v| as_string(v)).unwrap_or_default();

        if code_content.is_empty() {
            tracing::error!("QR code content is empty");
            return Ok(None);
        }

        let mut result = HashMap::new();
        result.insert("t".to_string(), t);
        result.insert("ck".to_string(), ck);
        result.insert("content".to_string(), code_content);
        Ok(Some(result))
    }

    /// Poll QR code scan status.
    pub async fn qrcode_poll(&self, t: &str, ck: &str) -> Result<HashMap<String, String>> {
        let mut params = {
            let poll_guard = self.poll_params.lock().await;
            poll_guard.clone().unwrap_or_default()
        };
        params.insert("t".to_string(), t.to_string());
        params.insert("ck".to_string(), ck.to_string());

        tracing::debug!("qrcode_poll params: {:?}", params);

        let url = "https://passport.goofish.com/newlogin/qrcode/query.do";
        let resp = self.client.post(url).form(&params).send().await?;

        // Extract set-cookie headers before consuming resp (for CONFIRMED flow)
        let set_cookie_headers: Vec<_> = resp
            .headers()
            .get_all("set-cookie")
            .iter()
            .map(|v| v.to_str().unwrap_or("").to_string())
            .collect();

        let body: Value = resp.json().await?;
        let data = body
            .get("content")
            .and_then(|c| c.get("data"))
            .and_then(|d| d.as_object())
            .cloned()
            .unwrap_or_default();

        // Check for error/redirect (risk control)
        if let Some(redirect) = data.get("iframeRedirect").and_then(|v| v.as_str()) {
            if !redirect.is_empty() {
                let mut result = HashMap::new();
                result.insert("status".to_string(), "ERROR".to_string());
                result.insert("redirect_url".to_string(), redirect.to_string());
                return Ok(result);
            }
        }

        let status = data
            .get("qrCodeStatus")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN")
            .to_string();

        // If confirmed, save cookies
        if status == "CONFIRMED" {
            let mut cookie_updated = false;
            let mut cookies = self.auth.cookies.lock().await;
            for cookie_str in &set_cookie_headers {
                let pure = cookie_str.split(';').next().unwrap_or("").trim().to_string();
                if let Some(eq) = pure.find('=') {
                    let k = pure[..eq].trim().to_string();
                    let v = pure[eq + 1..].trim().to_string();
                    if cookies.get(&k).map(|s| s.as_str()) != Some(&v) {
                        cookies.insert(k, v);
                        cookie_updated = true;
                    }
                }
            }
            drop(cookies);
            if cookie_updated {
                self.auth.save_cookies_to_file().await;
            }
        }

        let mut result = HashMap::new();
        result.insert("status".to_string(), status);
        Ok(result)
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
            poll_params: Mutex::new(None),
        }
    }
}

impl BaseAPI for FishAPI {}

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
