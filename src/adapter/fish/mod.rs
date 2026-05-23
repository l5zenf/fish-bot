use crate::adapter::BaseAdapter;
use crate::error::{AppError, Result};
use crate::model::{Message, MessageEvent};
use crate::plugin;
use crate::protocol::decode_message;
use async_trait::async_trait;
use futures::stream::SplitStream;
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::{interval, sleep, Duration};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::MaybeTlsStream;

pub mod sign;
pub mod auth;
pub mod api;

use api::FishAPI;
use auth::AuthManager;
use sign::{generate_mid, generate_uuid};

type WsWriter = futures::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
    WsMessage,
>;
type WsReader = SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>;

pub struct FishWebSocketAdapter {
    callback: std::sync::Mutex<Option<Box<dyn Fn(MessageEvent) + Send + Sync>>>,
    api: FishAPI,
    ws_writer: RwLock<Option<WsWriter>>,
}

impl FishWebSocketAdapter {
    pub fn new() -> Self {
        let auth = AuthManager::new();
        let api = FishAPI::new(auth);
        Self {
            callback: std::sync::Mutex::new(None),
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

    /// Main connection logic: connect → handshake → receive loop.
    async fn connect_and_run(self: &Arc<Self>) -> Result<()> {
        let _cookie_str = self.api.cookies_str();
        let url = "wss://wss-goofish.dingtalk.com/";
        tracing::info!("Connecting to {}", url);

        use tokio_tungstenite::connect_async;
        let (ws_stream, _) = connect_async(url).await?;
        let (writer, reader) = ws_stream.split();

        {
            let mut guard = self.ws_writer.write().await;
            *guard = Some(writer);
        }

        // --- Step 1: Get access token ---
        let token = self.api.get_access_token().await?;
        tracing::info!("Got access token");

        // --- Step 2: Send /reg message ---
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

        // --- Step 3: Send sync status ackDiff ---
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

        // --- Step 4: Spawn heartbeat ---
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

        // --- Step 5: Receive loop ---
        self.receive_loop(reader).await
    }

    /// Receive loop: read messages, ack, decrypt, dispatch.
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

    /// Handle a single raw WS message frame.
    async fn handle_raw_message(self: &Arc<Self>, text: &str) -> Result<()> {
        let msg: Value = serde_json::from_str(text)?;

        // Send ACK (200) back with headers
        if let Some(headers) = msg.get("headers") {
            let mut ack_headers = serde_json::json!({
                "mid": headers.get("mid").and_then(|v| v.as_str()).unwrap_or(&generate_mid()),
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

            // Try to decrypt: first attempt as encrypted, fallback to base64->JSON
            let decrypted = match sign::decrypt(raw_data) {
                Ok(v) => v,
                Err(_) => {
                    // Fallback: base64 decode and parse as JSON
                    match base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        raw_data,
                    ) {
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
            self.dispatch_decrypted(decrypted).await?;
        }

        Ok(())
    }

    /// Parse a decrypted payload into a MessageEvent and dispatch.
    async fn dispatch_decrypted(self: &Arc<Self>, payload: Value) -> Result<()> {
        // The payload structure uses numbered fields (1, 2, 5, 6, 10) from the fish protocol.
        // Try to extract the inner message content.
        let body = payload.get("1").or_else(|| payload.get("body")).or(Some(&payload));
        let body = body.ok_or_else(|| AppError::Protocol("No body in decrypted payload".into()))?;

        // Extract sender info from field 10
        let sender_field = body.get("10").or_else(|| body.get("sender"));

        // Extract cid from field 2
        let cid_raw = body
            .get("2")
            .and_then(|v| v.as_str())
            .or_else(|| body.get("cid").and_then(|v| v.as_str()))
            .unwrap_or("");
        let cid = cid_raw.split('@').next().unwrap_or("").to_string();

        // Extract sender ID from field 10 -> senderUserId
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

        let messages = match content {
            Some(c) => {
                // content has contentType and the actual data
                if c.get("contentType").is_some() || c.get("content_type").is_some() {
                    vec![decode_message(c).unwrap_or(Message::Unknown)]
                } else if let Some(arr) = c.as_array() {
                    arr.iter()
                        .filter_map(|m| decode_message(m).ok())
                        .collect()
                } else {
                    vec![decode_message(c).unwrap_or(Message::Unknown)]
                }
            }
            None => vec![Message::Unknown],
        };

        let event = MessageEvent::new(cid, sender_id, sender_name, messages, payload);

        // Call user callback
        if let Some(ref cb) = *self.callback.lock().unwrap() {
            cb(event.clone());
        }

        // Dispatch to plugins
        let adapter: Arc<dyn BaseAdapter> = self.clone();
        plugin::dispatch_event(&event, adapter).await;

        Ok(())
    }
}

impl Clone for FishWebSocketAdapter {
    fn clone(&self) -> Self {
        // We don't clone the WS writer — it will be set again in connect_and_run
        Self {
            callback: std::sync::Mutex::new(None),
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

    async fn send(&self, target_id: &str, message: &Message, cid: Option<&str>) -> Result<()> {
        let (payload, custom_type) = encode_with_type(message)?;
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
        let ensure_msg = "Login/auth flow will be implemented when AuthManager is complete";

        // Clone self into an Arc so we can share across tasks
        let arc_self = Arc::new(self.clone());
        loop {
            tracing::info!("Starting adapter ({}). Connecting...", ensure_msg);
            if let Err(e) = arc_self.connect_and_run().await {
                tracing::error!("Connection error: {}, reconnecting in 5s...", e);
            } else {
                tracing::info!("Connection closed cleanly, reconnecting in 5s...");
            }
            sleep(Duration::from_secs(5)).await;
        }
    }
}

/// Encode a Message to its payload Value + contentType integer.
/// This is a companion to protocol::encode_message that returns (payload, content_type).
fn encode_with_type(msg: &Message) -> Result<(Value, i64)> {
    match msg {
        Message::Text { text } => Ok((
            serde_json::json!({"contentType": 1, "text": {"text": text}}),
            1,
        )),
        Message::Image { url, width, height } => Ok((
            serde_json::json!({
                "contentType": 2,
                "image": { "pics": [{"type": 0, "url": url, "width": width, "height": height}] }
            }),
            2,
        )),
        Message::Audio { url, duration_ms } => Ok((
            serde_json::json!({
                "contentType": 3,
                "audio": { "url": url, "duration": duration_ms }
            }),
            3,
        )),
        Message::Custom { segments } => {
            let data = STANDARD.encode(
                serde_json::to_string(
                    &segments
                        .iter()
                        .map(|s| match s {
                            Message::Text { text } => serde_json::json!({"type":"text","text":text}),
                            Message::Image { url, .. } => serde_json::json!({"type":"image","image_url":url}),
                            _ => serde_json::json!({"type":"unknown"}),
                        })
                        .collect::<Vec<_>>(),
                )?
                .as_bytes(),
            );
            Ok((
                serde_json::json!({
                    "contentType": 101,
                    "custom": { "type": 2, "data": data }
                }),
                2,
            ))
        }
        _ => Err(AppError::Protocol("Unsupported message type for send".into())),
    }
}
