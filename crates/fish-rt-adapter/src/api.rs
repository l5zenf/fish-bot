use fish_core::error::{AppError, Result};

fn http_err(e: reqwest::Error) -> AppError {
    AppError::http(e.to_string())
}
use crate::auth::AuthManager;
use crate::sign::{generate_device_id, generate_sign};
use reqwest::header::HeaderValue;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use urlencoding;

/// Fish API client wrapping the MTOP (Mobile Taobao Open Platform) protocol.
pub(crate) struct FishAPI {
    client: reqwest::Client,
    auth: AuthManager,
    device_id: String,
    /// Login form params for QR code flow, shared between gen and poll.
    poll_params: Mutex<Option<HashMap<String, String>>>,
}

#[allow(dead_code)]
impl FishAPI {
    pub(crate) fn new(auth: AuthManager) -> Self {
        let device_id = generate_device_id("");
        Self {
            client: reqwest::Client::builder()
                .cookie_store(true)
                .default_headers({
                    let mut headers = reqwest::header::HeaderMap::new();
                    headers.insert(
                        reqwest::header::ACCEPT,
                        HeaderValue::from_static("application/json"),
                    );
                    headers.insert(
                        reqwest::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/x-www-form-urlencoded"),
                    );
                    headers.insert(
                        reqwest::header::ORIGIN,
                        HeaderValue::from_static("https://www.goofish.com"),
                    );
                    headers.insert(
                        reqwest::header::REFERER,
                        HeaderValue::from_static("https://www.goofish.com/"),
                    );
                    headers.insert(
                        reqwest::header::USER_AGENT,
                        HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0"),
                    );
                    headers.insert(
                        reqwest::header::ACCEPT_LANGUAGE,
                        HeaderValue::from_static("zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6"),
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

    pub(crate) fn device_id(&self) -> String {
        self.device_id.clone()
    }

    pub(crate) async fn cookies_str(&self) -> String {
        self.auth.cookie_header().await
    }

    pub(crate) async fn my_id(&self) -> String {
        self.auth.my_id().await
    }

    pub(crate) fn auth(&self) -> &AuthManager {
        &self.auth
    }

    // ---- Core MTOP call ----

    /// Make an MTOP API call with full sign and cookie handling.
    pub(crate) async fn call_mtop(
        &self,
        api: &str,
        version: &str,
        data: &Value,
        extra_params: Option<&HashMap<String, String>>,
    ) -> Result<Value> {
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let t_str = t.to_string();
        let data_val = if data.is_null() || data.as_object().is_none_or(|o| o.is_empty()) {
            "{}".to_string()
        } else {
            serde_json::to_string(data)?
        };

        // Get token from cookies
        let cookies: HashMap<String, String> = self.auth.get_cookies().await;
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

        let body = format!("data={}", percent_encode(&data_val));

        tracing::debug!("MTOP call: {}?{:?}", url, params);

        let response = self
            .client
            .post(&url)
            .query(&params)
            .header("cookie", self.auth.cookie_header().await)
            .body(body)
            .send()
            .await
            .map_err(http_err)?;

        // Update cookies from response headers
        self.save_cookies_from_response(&response).await;

        let json: Value = response.json().await.map_err(http_err)?;

        // Check for error ret
        if let Some(ret) = json.get("ret").and_then(|v| v.as_array())
            && ret
                .iter()
                .all(|r| !r.as_str().is_some_and(|s| s.contains("SUCCESS")))
        {
            tracing::warn!("MTOP API error: {:?}", ret);
        }

        Ok(json)
    }

    /// Extract and persist cookies from a response's Set-Cookie headers.
    pub(crate) async fn save_cookies_from_response(&self, response: &reqwest::Response) {
        let mut cookie_updated = false;
        let mut cookies = self.auth.cookies.lock().await;

        for cookie_header in response.headers().get_all("set-cookie") {
            if let Ok(cookie_str) = cookie_header.to_str() {
                let pure = cookie_str
                    .split(';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if let Some(eq) = pure.find('=') {
                    let k = pure[..eq].trim().to_string();
                    let v = pure[eq + 1..].trim().to_string();
                    if cookies.get(&k).map(|s: &String| s.as_str()) != Some(&v) {
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
    pub(crate) async fn get_token(&self) -> Result<Value> {
        let data = serde_json::json!({
            "appKey": "444e9908a51d1cb236a27862abc769c9",
            "deviceId": self.device_id,
        });
        let mut extra = HashMap::new();
        extra.insert(
            "spm_pre".to_string(),
            "a21ybx.item.want.1.14ad3da6ALVq3n".to_string(),
        );
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
            .ok_or_else(|| AppError::auth("No accessToken in response"))
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
            .await
            .map_err(http_err)?;

        let html = resp.text().await.map_err(http_err)?;

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
                            if ch == '{' {
                                depth += 1;
                            } else if ch == '}' {
                                depth -= 1;
                            }
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
        let resp = self
            .client
            .get(url)
            .query(&params)
            .send()
            .await
            .map_err(http_err)?;

        let body: Value = resp.json().await.map_err(http_err)?;

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

        let t = data.get("t").map(&as_string).unwrap_or_default();
        let ck = data.get("ck").map(&as_string).unwrap_or_default();
        let code_content = data.get("codeContent").map(as_string).unwrap_or_default();

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
        let resp = self
            .client
            .post(url)
            .form(&params)
            .send()
            .await
            .map_err(http_err)?;

        // Extract set-cookie headers before consuming resp (for CONFIRMED flow)
        let set_cookie_headers: Vec<_> = resp
            .headers()
            .get_all("set-cookie")
            .iter()
            .map(|v| v.to_str().unwrap_or("").to_string())
            .collect();

        let body: Value = resp.json().await.map_err(http_err)?;
        let data = body
            .get("content")
            .and_then(|c| c.get("data"))
            .and_then(|d| d.as_object())
            .cloned()
            .unwrap_or_default();

        // Check for error/redirect (risk control)
        if let Some(redirect) = data.get("iframeRedirect").and_then(|v| v.as_str())
            && !redirect.is_empty()
        {
            let mut result = HashMap::new();
            result.insert("status".to_string(), "ERROR".to_string());
            result.insert("redirect_url".to_string(), redirect.to_string());
            return Ok(result);
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
                let pure = cookie_str
                    .split(';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if let Some(eq) = pure.find('=') {
                    let k = pure[..eq].trim().to_string();
                    let v = pure[eq + 1..].trim().to_string();
                    if cookies.get(&k).map(|s: &String| s.as_str()) != Some(&v) {
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
        extra.insert(
            "spm_pre".to_string(),
            "a21ybx.home.nav.1.62953da6OYFsax".to_string(),
        );
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
        extra.insert(
            "spm_pre".to_string(),
            "a21ybx.home.nav.1.62953da6OYFsax".to_string(),
        );
        extra.insert("log_id".to_string(), "62953da6OYFsax".to_string());
        self.call_mtop("mtop.idle.web.xyh.item.list", "1.0", &data, Some(&extra))
            .await
    }

    pub async fn get_item_info(&self, item_id: &str) -> Result<Value> {
        let data = serde_json::json!({ "itemId": item_id });
        let mut extra = HashMap::new();
        extra.insert(
            "spm_pre".to_string(),
            "a21ybx.item.want.1.12523da6waCtUp".to_string(),
        );
        extra.insert("log_id".to_string(), "12523da6waCtUp".to_string());
        self.call_mtop("mtop.taobao.idle.pc.detail", "1.0", &data, Some(&extra))
            .await
    }

    // ---- Auth orchestration ----

    /// Ensure we have valid authentication before connecting.
    pub async fn ensure_auth(&self) -> Result<()> {
        let cookies: HashMap<String, String> = self.auth.get_cookies().await;

        if cookies.contains_key("unb") {
            tracing::info!("Found local auth cookies, validating...");
            match self.get_token().await {
                Ok(res) => {
                    let has_access_token = res
                        .get("data")
                        .and_then(|d| d.get("accessToken"))
                        .and_then(|v| v.as_str())
                        .is_some();

                    if has_access_token {
                        let unb = cookies.get("unb").cloned().unwrap_or_default();
                        let nick = cookies.get("tracknick").cloned().unwrap_or_default();
                        let nick = urlencoding::decode(&nick)
                            .map(|s| s.to_string())
                            .unwrap_or(nick);
                        tracing::info!("Successfully logged in as {} ({})", nick, unb);
                        return Ok(());
                    }

                    let ret_str = res.to_string();
                    if ret_str.contains("FAIL_SYS_SESSION_EXPIRED") {
                        tracing::warn!("Session expired, need to re-login");
                        self.auth.rm_auth_file().await;
                        {
                            let mut c = self.auth.cookies.lock().await;
                            c.clear();
                        }
                    } else if ret_str.contains("FAIL_SYS_USER_VALIDATE") {
                        let url = res
                            .get("data")
                            .and_then(|d| d.get("url"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        tracing::error!(
                            "Risk control triggered! Please complete CAPTCHA in browser: {}",
                            url
                        );
                        return Err(AppError::auth(
                            "Risk control triggered, manual CAPTCHA required",
                        ));
                    } else {
                        tracing::warn!("Token invalid, trying to refresh...");
                        match self.get_token().await {
                            Ok(refresh_res)
                                if refresh_res
                                    .get("data")
                                    .and_then(|d| d.get("accessToken"))
                                    .is_some() =>
                            {
                                tracing::info!("Token refreshed successfully");
                                return Ok(());
                            }
                            _ => {
                                tracing::warn!("Token refresh failed, need to re-login");
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to validate auth: {}, proceeding to QR login", e);
                }
            }
        } else {
            tracing::info!("No local auth cookies found");
        }

        self.qrcode_login_flow().await
    }

    /// Full QR code login flow: get mh5tk -> generate QR -> display -> poll -> save cookies.
    pub async fn qrcode_login_flow(&self) -> Result<()> {
        tracing::info!("Starting QR code login flow...");
        println!("\n  Please scan the QR code with the Xianyu (闲鱼) app to log in.\n");

        let _ = self.get_mh5tk().await?;
        tracing::info!("Got mh5tk cookies");

        let qr_data = self
            .qrcode_gen()
            .await?
            .ok_or_else(|| AppError::auth("Failed to generate QR code"))?;

        let content = qr_data
            .get("content")
            .ok_or_else(|| AppError::auth("QR code content missing"))?;

        match qrcode::QrCode::new(content.as_bytes()) {
            Ok(code) => {
                let image = code
                    .render::<qrcode::render::unicode::Dense1x2>()
                    .dark_color(qrcode::render::unicode::Dense1x2::Dark)
                    .light_color(qrcode::render::unicode::Dense1x2::Light)
                    .build();
                println!("{}", image);
            }
            Err(e) => {
                tracing::warn!("Failed to render QR code: {}, showing URL instead", e);
                println!("QR Code URL: {}", content);
            }
        }

        let t = qr_data.get("t").cloned().unwrap_or_default();
        let ck = qr_data.get("ck").cloned().unwrap_or_default();

        let mut is_scanned = false;
        loop {
            sleep(Duration::from_millis(1500)).await;

            let result = self.qrcode_poll(&t, &ck).await?;
            let status = result
                .get("status")
                .map(|s| s.as_str())
                .unwrap_or("UNKNOWN");

            match status {
                "CONFIRMED" => {
                    tracing::info!("Login confirmed! Session saved.");
                    println!("  Login successful!");
                    return Ok(());
                }
                "NEW" => continue,
                "SCANED" => {
                    if !is_scanned {
                        is_scanned = true;
                        tracing::info!("QR code scanned, waiting for confirmation on phone...");
                        println!("  QR code scanned! Please confirm login on your phone.");
                    }
                }
                "EXPIRED" => {
                    tracing::warn!("QR code expired");
                    return Err(AppError::auth("QR code expired, please restart"));
                }
                "CANCELED" => {
                    tracing::info!("User cancelled login on phone");
                    return Err(AppError::auth("Login cancelled"));
                }
                "ERROR" => {
                    let redirect = result.get("redirect_url").cloned().unwrap_or_default();
                    tracing::warn!(
                        "Account is risk-controlled. Please visit URL to verify via SMS: {}",
                        redirect
                    );
                    return Err(AppError::auth(format!(
                        "Risk control: verify at {}",
                        redirect
                    )));
                }
                _ => {
                    tracing::debug!("Unknown QR status: {}", status);
                }
            }
        }
    }
}

impl Clone for FishAPI {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            auth: self.auth.clone(),
            device_id: self.device_id.clone(),
            poll_params: Mutex::new(None),
        }
    }
}


/// Simple URL encoder for form bodies.
fn percent_encode(input: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t3_42_percent_encode_alphanumeric() {
        assert_eq!(percent_encode("hello123"), "hello123");
        assert_eq!(percent_encode("ABC-DEF_ghi.~"), "ABC-DEF_ghi.~");
    }

    #[test]
    fn t3_43_percent_encode_spaces() {
        assert_eq!(percent_encode("hello world"), "hello%20world");
        assert_eq!(percent_encode("a b c"), "a%20b%20c");
    }

    #[test]
    fn t3_44_percent_encode_special_chars() {
        let encoded = percent_encode("{\"key\":\"value\"}");
        assert!(encoded.contains("%7B"));
        assert!(encoded.contains("%22"));
        assert!(encoded.contains("%3A"));
        assert!(encoded.contains("%7D"));
    }

    #[test]
    fn t3_45_percent_encode_empty() {
        assert_eq!(percent_encode(""), "");
    }

    #[test]
    fn t3_46_percent_encode_chinese() -> anyhow::Result<()> {
        let encoded = percent_encode("你好");
        assert!(
            !encoded.contains("你好"),
            "Chinese chars should be percent-encoded"
        );
        assert!(
            encoded.len() > 2,
            "encoded form should be longer than raw UTF-8"
        );
        Ok(())
    }

    #[tokio::test]
    async fn t3_47_http_err_creates_app_error() -> anyhow::Result<()> {
        // Force a connection error to get a reqwest::Error
        if let Err(e) = reqwest::Client::new()
            .get("http://127.0.0.1:1")
            .timeout(std::time::Duration::from_millis(1))
            .send()
            .await
        {
            let app_err = http_err(e);
            assert!(app_err.to_string().contains("HTTP"), "should contain HTTP");
        }
        Ok(())
    }

    // ---- FishAPI getter / constructor tests ----

    fn test_auth() -> AuthManager {
        AuthManager::new()
    }

    #[test]
    fn t3_59_api_new_creates_with_device_id() -> anyhow::Result<()> {
        let api = FishAPI::new(test_auth());
        assert!(!api.device_id().is_empty(), "device_id should be non-empty");
        Ok(())
    }

    #[tokio::test]
    async fn t3_60_api_cookies_str_does_not_panic() -> anyhow::Result<()> {
        let api = FishAPI::new(test_auth());
        let _cookies = api.cookies_str().await;
        // cookies may or may not be present depending on environment
        Ok(())
    }

    #[tokio::test]
    async fn t3_61_api_my_id_does_not_panic() -> anyhow::Result<()> {
        let api = FishAPI::new(test_auth());
        let _id = api.my_id().await;
        // my_id may or may not be set depending on environment
        Ok(())
    }

    #[test]
    fn t3_62_api_clone_preserves_device_id() -> anyhow::Result<()> {
        let api = FishAPI::new(test_auth());
        let did = api.device_id();
        let cloned = api.clone();
        assert_eq!(cloned.device_id(), did);
        Ok(())
    }

    #[tokio::test]
    async fn t3_63_api_clone_has_independent_poll_params() -> anyhow::Result<()> {
        let api = FishAPI::new(test_auth());
        {
            let mut pp = api.poll_params.lock().await;
            *pp = Some([("key".into(), "val".into())].into());
        }
        let cloned = api.clone();
        let cloned_pp = cloned.poll_params.lock().await;
        assert!(
            cloned_pp.is_none(),
            "cloned api should have independent None poll_params"
        );
        Ok(())
    }

    #[test]
    fn t3_64_api_auth_returns_ref() -> anyhow::Result<()> {
        let api = FishAPI::new(test_auth());
        let _auth: &AuthManager = api.auth();
        Ok(())
    }

    #[test]
    fn t3_65_api_new_with_fresh_auth() -> anyhow::Result<()> {
        let api = FishAPI::new(test_auth());
        assert!(!api.device_id().is_empty());
        // Verify we can create a second API with a different auth
        let api2 = FishAPI::new(test_auth());
        assert!(!api2.device_id().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn t3_66_api_new_creates_client() -> anyhow::Result<()> {
        let api = FishAPI::new(test_auth());
        // Verify that poll_params is initialized to None
        let pp = api.poll_params.lock().await;
        assert!(pp.is_none(), "poll_params should be None initially");
        Ok(())
    }

    #[test]
    fn t3_67_percent_encode_special_all() -> anyhow::Result<()> {
        // Test all special characters that should be encoded
        let encoded = percent_encode("!@#$%^&*()+=[]{}|;:',<>?/`\"");
        // All these chars should be percent-encoded
        assert!(!encoded.contains('!'));
        assert!(!encoded.contains('@'));
        assert!(!encoded.contains('#'));
        Ok(())
    }
}
