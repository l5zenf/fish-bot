use crate::adapter::BaseAdapter;
use fish_core::error::{AppError, Result};
use fish_core::event::MessageEvent;
use fish_core::message::MessageChain;
use async_trait::async_trait;
use futures::stream::SplitStream;
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::{interval, sleep, Duration};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::MaybeTlsStream;

pub mod sign;
pub mod auth;
pub mod api;
pub mod protocol;

use api::FishAPI;
use auth::AuthManager;
use sign::{generate_mid, generate_uuid};
use protocol::{decode_content, encode_chain};

type WsWriter = futures::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
    WsMessage,
>;
type WsReader = SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Fish WebSocket adapter, matching Python adapters/fish/__init__.py FishWebSocketAdapter.
pub struct FishWebSocketAdapter {
    callback: Mutex<Option<Box<dyn Fn(MessageEvent) + Send + Sync>>>,
    api: FishAPI,
    ws_writer: RwLock<Option<WsWriter>>,
}

impl FishWebSocketAdapter {
    pub fn new() -> Self {
        let auth = AuthManager::new();
        let api = FishAPI::new(auth);
        Self {
            callback: Mutex::new(None),
            api,
            ws_writer: RwLock::new(None),
        }
    }

    /// Send a JSON Value over the active WebSocket writer.
    async fn send_ws(&self, msg: &Value) -> Result<()> {
        let mut writer = self.ws_writer.write().await;
        match writer.as_mut() {
            Some(w) => {
                let text = serde_json::to_string(msg)?;
                w.send(WsMessage::Text(text))
                    .await
                    .map_err(AppError::Ws)?;
                Ok(())
            }
            None => Err(AppError::Protocol("WebSocket not connected".into())),
        }
    }

    /// Main connection logic: connect -> handshake -> receive loop.
    async fn connect_and_run(self: &Arc<Self>) -> Result<()> {
        let url = "wss://wss-goofish.dingtalk.com/";
        tracing::info!("Connecting to {}", url);

        use tokio_tungstenite::connect_async;
        let (ws_stream, _) = connect_async(url).await?;
        let (writer, reader) = ws_stream.split();

        {
            let mut guard = self.ws_writer.write().await;
            *guard = Some(writer);
        }

        // Step 1: Get access token
        let token = self.api.get_access_token().await?;
        tracing::info!("Got access token");

        // Step 2: Send /reg message
        let reg_msg = serde_json::json!({
            "lwp": "/reg",
            "headers": {
                "cache-header": "app-key token ua wv",
                "app-key": "444e9908a51d1cb236a27862abc769c9",
                "token": token,
                "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 DingTalk(2.1.5) OS(Windows/10) Browser(Chrome/133.0.0.0) DingWeb/2.1.5 IMPaaS DingWeb/2.1.5",
                "dt": "j",
                "wv": "im:3,au:3,sy:6",
                "did": self.api.device_id(),
                "mid": generate_mid(),
            }
        });
        self.send_ws(&reg_msg).await?;
        tracing::info!("Sent /reg");

        // Step 3: Send sync status ackDiff
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let sync_msg = serde_json::json!({
            "lwp": "/r/SyncStatus/ackDiff",
            "headers": { "mid": generate_mid() },
            "body": [
                {
                    "pipeline": "sync",
                    "tooLong2Tag": "PNM,1",
                    "channel": "sync",
                    "topic": "sync",
                    "highPts": 0,
                    "pts": now * 1000,
                    "seq": 0,
                    "timestamp": now,
                }
            ]
        });
        self.send_ws(&sync_msg).await?;
        tracing::info!("Sent sync ackDiff");

        // Step 4: Spawn heartbeat
        let hb_self = self.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(15));
            loop {
                ticker.tick().await;
                let hb = serde_json::json!({
                    "lwp": "/!",
                    "headers": { "mid": generate_mid() }
                });
                if hb_self.send_ws(&hb).await.is_err() {
                    break;
                }
                tracing::debug!("Heartbeat sent");
            }
        });

        // Step 5: Receive loop
        self.receive_loop(reader).await
    }

    /// Receive loop: read messages, ack, decrypt, construct event, invoke callback.
    async fn receive_loop(self: &Arc<Self>, mut reader: WsReader) -> Result<()> {
        while let Some(msg_result) = reader.next().await {
            let text = match msg_result {
                Ok(WsMessage::Text(t)) => t,
                Ok(WsMessage::Close(frame)) => {
                    tracing::info!("WS closed: {:?}", frame);
                    break;
                }
                Ok(_) => continue,
                Err(e) => {
                    tracing::error!("WS recv error: {}", e);
                    break;
                }
            };
            if let Err(e) = self.handle_raw_message(&text).await {
                tracing::error!("handle_raw_message: {}", e);
            }
        }
        Ok(())
    }

    /// Handle a single raw WS message frame, matching Python _handle_raw_message.
    async fn handle_raw_message(self: &Arc<Self>, text: &str) -> Result<()> {
        let msg: Value = serde_json::from_str(text)?;

        // Send ACK (200) back with headers
        if let Some(headers) = msg.get("headers") {
            let mut ack_headers = serde_json::json!({
                "mid": headers.get("mid").and_then(|v| v.as_str()).unwrap_or(""),
                "sid": headers.get("sid").and_then(|v| v.as_str()).unwrap_or(""),
            });
            for key in &["app-key", "ua", "dt"] {
                if let Some(val) = headers.get(*key) {
                    ack_headers[key] = val.clone();
                }
            }
            let ack = serde_json::json!({ "code": 200, "headers": ack_headers });
            let _ = self.send_ws(&ack).await;
        }

        // Only process syncPushPackage messages
        if !text.contains("syncPushPackage") {
            return Ok(());
        }

        let body = msg.get("body").and_then(|v| v.as_object()).cloned().unwrap_or_default();
        let push_pkg = match body.get("syncPushPackage") {
            Some(v) => v.clone(),
            None => return Ok(()),
        };
        let data_list = push_pkg.get("data").and_then(|v| v.as_array()).cloned().unwrap_or_default();

        for item in &data_list {
            let raw_data = match item.get("data").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };

            // Try to decrypt: first attempt as encrypted, fallback to base64 -> JSON
            let decrypted = match sign::decrypt(raw_data) {
                Ok(v) => v,
                Err(_) => {
                    match STANDARD.decode(raw_data.as_bytes()) {
                        Ok(bytes) => {
                            match serde_json::from_slice(&bytes) {
                                Ok(v) => v,
                                Err(_) => continue,
                            }
                        }
                        Err(_) => continue,
                    }
                }
            };

            // Parse decrypted message into MessageEvent
            if let Some(event) = self.parse_event(&decrypted).await {
                tracing::info!(
                    "[接收] <- {}({}): {}",
                    event.sender_name,
                    event.sender_id,
                    event.summary()
                );

                // Invoke callback (set by Bot)
                if let Some(ref cb) = *self.callback.lock().unwrap() {
                    cb(event);
                }
            }
        }

        Ok(())
    }

    /// Parse a decrypted payload into a MessageEvent, matching Python _handle_raw_message parsing.
    async fn parse_event(self: &Arc<Self>, payload: &Value) -> Option<MessageEvent> {
        let body = payload.get("1").or_else(|| payload.get("body")).or(Some(payload))?;

        // Extract sender info from field 10
        let sender_field = body.get("10").or_else(|| body.get("sender"));

        // Extract cid from field 2
        let cid_raw = body
            .get("2")
            .and_then(|v| v.as_str())
            .or_else(|| body.get("cid").and_then(|v| v.as_str()))
            .unwrap_or("");
        let cid = cid_raw.split('@').next().unwrap_or("").to_string();

        // Extract sender ID
        let sender_id = sender_field
            .and_then(|s| s.get("senderUserId").or_else(|| s.get("user_id")))
            .and_then(|v| v.as_str())
            .or_else(|| {
                body.get("sender_id")
                    .or_else(|| payload.get("sender_id"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("")
            .to_string();

        let sender_name = sender_field
            .and_then(|s| s.get("reminderTitle").or_else(|| s.get("name")))
            .and_then(|v| v.as_str())
            .or_else(|| {
                body.get("sender_name")
                    .or_else(|| payload.get("sender_name"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("")
            .to_string();

        // Extract message content from field 6
        let content = body.get("6").or_else(|| body.get("content")).or_else(|| {
            body.get("3")
                .and_then(|v3| v3.get("5"))
                .or_else(|| payload.get("messages"))
        });

        let segments = match content {
            Some(c) => decode_content(c).unwrap_or_default(),
            None => Vec::new(),
        };

        let messages = MessageChain::from(segments);

        // Skip messages from self
        let my_id = self.api.my_id().await;
        if sender_id == my_id {
            return None;
        }

        Some(MessageEvent::new(
            cid,
            sender_id,
            sender_name,
            messages,
            payload.clone(),
        ))
    }

    /// Ensure we have valid authentication before connecting.
    async fn ensure_auth(&self) -> Result<()> {
        let cookies: HashMap<String, String> = self.api.auth().get_cookies().await;

        if cookies.contains_key("unb") {
            tracing::info!("Found local auth cookies, validating...");
            match self.api.get_token().await {
                Ok(res) => {
                    let has_access_token = res
                        .get("data")
                        .and_then(|d| d.get("accessToken"))
                        .and_then(|v| v.as_str())
                        .is_some();

                    if has_access_token {
                        let unb = cookies.get("unb").cloned().unwrap_or_default();
                        let nick = cookies
                            .get("tracknick")
                            .cloned()
                            .unwrap_or_default();
                        let nick = urlencoding::decode(&nick)
                            .map(|s| s.to_string())
                            .unwrap_or(nick);
                        tracing::info!("Successfully logged in as {} ({})", nick, unb);
                        return Ok(());
                    }

                    let ret_str = res.to_string();
                    if ret_str.contains("FAIL_SYS_SESSION_EXPIRED") {
                        tracing::warn!("Session expired, need to re-login");
                        self.api.auth().rm_auth_file().await;
                        {
                            let mut c = self.api.auth().cookies.lock().await;
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
                        return Err(AppError::Auth(
                            "Risk control triggered, manual CAPTCHA required".into(),
                        ));
                    } else {
                        tracing::warn!("Token invalid, trying to refresh...");
                        match self.api.get_token().await {
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
    async fn qrcode_login_flow(&self) -> Result<()> {
        tracing::info!("Starting QR code login flow...");
        println!("\n  Please scan the QR code with the Xianyu (闲鱼) app to log in.\n");

        let _ = self.api.get_mh5tk().await?;
        tracing::info!("Got mh5tk cookies");

        let qr_data = self
            .api
            .qrcode_gen()
            .await?
            .ok_or_else(|| AppError::Auth("Failed to generate QR code".into()))?;

        let content = qr_data
            .get("content")
            .ok_or_else(|| AppError::Auth("QR code content missing".into()))?;

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

            let result = self.api.qrcode_poll(&t, &ck).await?;
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
                    return Err(AppError::Auth("QR code expired, please restart".into()));
                }
                "CANCELED" => {
                    tracing::info!("User cancelled login on phone");
                    return Err(AppError::Auth("Login cancelled".into()));
                }
                "ERROR" => {
                    let redirect = result.get("redirect_url").cloned().unwrap_or_default();
                    tracing::warn!(
                        "Account is risk-controlled. Please visit URL to verify via SMS: {}",
                        redirect
                    );
                    return Err(AppError::Auth(format!(
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

impl Clone for FishWebSocketAdapter {
    fn clone(&self) -> Self {
        Self {
            callback: Mutex::new(None),
            api: self.api.clone(),
            ws_writer: RwLock::new(None),
        }
    }
}

#[async_trait]
impl BaseAdapter for FishWebSocketAdapter {
    fn set_callback(&self, cb: Box<dyn Fn(MessageEvent) + Send + Sync>) {
        let mut guard = self.callback.lock().unwrap();
        *guard = Some(cb);
    }

    async fn send(
        &self,
        target_id: &str,
        message: &MessageChain,
        cid: Option<&str>,
    ) -> Result<()> {
        let (payload, custom_type) = encode_chain(message.segments())?;
        let encoded_data = STANDARD.encode(serde_json::to_string(&payload)?.as_bytes());
        let _cid = cid.unwrap_or(target_id);
        let my_id = self.api.my_id().await;

        let msg = serde_json::json!({
            "lwp": "/r/MessageSend/sendByReceiverScope",
            "headers": { "mid": generate_mid() },
            "body": [
                {
                    "uuid": generate_uuid(),
                    "cid": format!("{}@goofish", _cid),
                    "conversationType": 1,
                    "content": {
                        "contentType": 101,
                        "custom": { "type": custom_type, "data": encoded_data }
                    },
                    "redPointPolicy": 0,
                    "extension": { "extJson": "{}" },
                    "ctx": { "appVersion": "1.0", "platform": "web" },
                    "mtags": {},
                    "msgReadStatusSetting": 1,
                },
                { "actualReceivers": [format!("{}@goofish", target_id), format!("{}@goofish", my_id)] }
            ]
        });

        self.send_ws(&msg).await
    }

    async fn run(&self) -> Result<()> {
        // Ensure auth before connecting
        self.ensure_auth().await?;

        // Clone self into an Arc so we can share across tasks
        let arc_self = Arc::new(self.clone());
        loop {
            tracing::info!("Starting adapter. Connecting...");
            if let Err(e) = arc_self.connect_and_run().await {
                tracing::error!("Connection error: {}, reconnecting in 5s...", e);
            } else {
                tracing::info!("Connection closed cleanly, reconnecting in 5s...");
            }
            sleep(Duration::from_secs(5)).await;
        }
    }
}
