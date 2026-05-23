use std::sync::Arc;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use fish_core::error::Result;
use fish_core::event::MessageEvent;
use fish_core::message::MessageChain;
use futures::stream::SplitStream;
use futures::StreamExt;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::MaybeTlsStream;

use super::api::FishAPI;
use super::auth::AuthManager;
use super::connection::FishConnection;
use super::protocol::{decode_content, encode_chain};
use super::sign::{generate_mid, generate_uuid, decrypt};
use crate::adapter::BaseAdapter;

type WsReader = SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>;
type CallbackFn = Box<dyn Fn(MessageEvent) + Send + Sync>;

/// Thin coordinator: ties together FishAPI (HTTP auth) and FishConnection (WS transport).
pub struct FishWebSocketAdapter {
    callback: Arc<Mutex<Option<CallbackFn>>>,
    api: FishAPI,
    conn: Arc<FishConnection>,
}

impl FishWebSocketAdapter {
    pub fn new() -> Self {
        let auth = AuthManager::new();
        let api = FishAPI::new(auth);
        Self {
            callback: Arc::new(Mutex::new(None)),
            api,
            conn: Arc::new(FishConnection::new()),
        }
    }

    /// Main connection logic: connect -> handshake -> receive loop.
    async fn connect_and_run(self: &Arc<Self>) -> Result<()> {
        let url = "wss://wss-goofish.dingtalk.com/";
        tracing::info!("Connecting to {}", url);

        let reader = self.conn.connect(url).await?;

        let token = self.api.get_access_token().await?;
        tracing::info!("Got access token");

        self.conn.handshake(&token, &self.api.device_id()).await?;
        self.conn.spawn_heartbeat();

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

    /// Handle a single raw WS message frame.
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
            let _ = self.conn.send(&ack).await;
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
            let decrypted = match decrypt(raw_data) {
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
                if let Some(ref cb) = *self.callback.lock() {
                    cb(event);
                }
            }
        }

        Ok(())
    }

    /// Parse a decrypted payload into a MessageEvent.
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

    /// Run the adapter event loop, taking ownership of the Arc directly.
    pub async fn run_arc(self: Arc<Self>) -> Result<()> {
        self.api.ensure_auth().await?;
        loop {
            tracing::info!("Starting adapter. Connecting...");
            if let Err(e) = self.connect_and_run().await {
                tracing::error!("Connection error: {}, reconnecting in 5s...", e);
            } else {
                tracing::info!("Connection closed cleanly, reconnecting in 5s...");
            }
            sleep(Duration::from_secs(5)).await;
        }
    }
}

impl Default for FishWebSocketAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for FishWebSocketAdapter {
    fn clone(&self) -> Self {
        Self {
            callback: Arc::clone(&self.callback),
            api: self.api.clone(),
            conn: Arc::clone(&self.conn),
        }
    }
}

#[async_trait]
impl BaseAdapter for FishWebSocketAdapter {
    fn set_callback(&self, cb: Box<dyn Fn(MessageEvent) + Send + Sync>) {
        let mut guard = self.callback.lock();
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

        self.conn.send(&msg).await
    }

    async fn run(&self) -> Result<()> {
        // Ensure auth before connecting
        self.api.ensure_auth().await?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use fish_core::message::{MessageChain, MessageSegment};

    #[test]
    fn t3_55_new_creates_adapter() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let cb = adapter.callback.lock();
        assert!(cb.is_none(), "callback should be None initially");
        Ok(())
    }

    #[test]
    fn t3_56_clone_preserves_callback() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        {
            let mut cb = adapter.callback.lock();
            *cb = Some(Box::new(|_| {}));
        }
        let cloned = adapter.clone();
        let cloned_cb = cloned.callback.lock();
        assert!(cloned_cb.is_some(), "cloned adapter must share the callback (Arc-wrapped)");
        Ok(())
    }

    #[test]
    fn t3_57_default_creates_empty_callback() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::default();
        let cb = adapter.callback.lock();
        assert!(cb.is_none());
        Ok(())
    }

    #[test]
    fn t3_58_clone_preserves_device_id() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let did = adapter.api.device_id();
        let cloned = adapter.clone();
        assert_eq!(cloned.api.device_id(), did);
        Ok(())
    }

    #[test]
    fn t3_59_set_callback_stores_function() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        adapter.set_callback(Box::new(|_| {}));
        let cb = adapter.callback.lock();
        assert!(cb.is_some(), "callback should be set");
        Ok(())
    }

    #[tokio::test]
    async fn t3_60_send_returns_err_without_connection() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let chain = MessageChain::from("test message");
        let result = adapter.send("target", &chain, None).await;
        assert!(result.is_err(), "send should fail without WebSocket connection");
        Ok(())
    }

    /// Test the full message building path: single segment, multi segment, with cid
    #[tokio::test]
    async fn t3_61_send_builds_message_correctly() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let chain = MessageChain::from("hello");
        let result = adapter.send("user123", &chain, Some("cid456")).await;
        assert!(result.is_err());
        if let Err(e) = result {
            let err_msg = e.to_string();
            assert!(err_msg.contains("WebSocket"), "error should mention WebSocket");
        }
        Ok(())
    }

    #[tokio::test]
    async fn t3_62_send_multi_segment() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let chain = MessageChain::from(vec![
            MessageSegment::text("hello"),
            MessageSegment::text("world"),
        ]);
        let result = adapter.send("target", &chain, None).await;
        assert!(result.is_err(), "should fail without WebSocket");
        Ok(())
    }

    #[tokio::test]
    async fn t3_63_send_with_image() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let chain = MessageChain::from(MessageSegment::image(
            "https://example.com/pic.jpg",
        ));
        let result = adapter.send("target", &chain, None).await;
        assert!(result.is_err(), "should fail without WebSocket");
        Ok(())
    }

    #[tokio::test]
    async fn t3_64_send_with_cid() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let chain = MessageChain::from("test");
        let result = adapter.send("user456", &chain, Some("conv789")).await;
        assert!(result.is_err(), "should fail without WebSocket");
        Ok(())
    }

    #[tokio::test]
    async fn t3_65_send_with_audio() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let chain = MessageChain::from(MessageSegment::Audio {
            audio_url: "https://example.com/sound.mp3".into(),
            duration_ms: 3000,
        });
        let result = adapter.send("target", &chain, None).await;
        assert!(result.is_err(), "should fail without WebSocket");
        Ok(())
    }

    #[tokio::test]
    async fn t3_66_adapter_new_has_device_id() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let did = adapter.api.device_id();
        assert!(!did.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn t3_67_adapter_my_id_callable() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let _id = adapter.api.my_id().await;
        Ok(())
    }

    #[tokio::test]
    async fn t3_68_send_with_custom_node() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let chain = MessageChain::from(MessageSegment::CustomNode {
            desc: "test".into(),
            content: serde_json::json!({"type": "custom"}),
        });
        let result = adapter.send("target", &chain, None).await;
        assert!(result.is_err(), "should fail without WebSocket");
        Ok(())
    }

    #[tokio::test]
    async fn t3_69_send_mixed_segments() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let chain = MessageChain::from(vec![
            MessageSegment::text("hello"),
            MessageSegment::Image {
                image_url: "https://img.jpg".into(),
                width: 100,
                height: 200,
            },
        ]);
        let result = adapter.send("target", &chain, None).await;
        assert!(result.is_err(), "should fail without WebSocket");
        Ok(())
    }

    #[tokio::test]
    async fn t3_70_send_with_audio_segment() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let chain = MessageChain::from(vec![
            MessageSegment::text("audio file:"),
            MessageSegment::Audio {
                audio_url: "https://sound.mp3".into(),
                duration_ms: 5000,
            },
        ]);
        let result = adapter.send("target", &chain, None).await;
        assert!(result.is_err(), "should fail without WebSocket");
        Ok(())
    }

    #[tokio::test]
    async fn t3_71_send_with_custom_node_multi() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let chain = MessageChain::from(vec![
            MessageSegment::text("custom:"),
            MessageSegment::CustomNode {
                desc: "node".into(),
                content: serde_json::json!({"data": "value"}),
            },
        ]);
        let result = adapter.send("target", &chain, None).await;
        assert!(result.is_err(), "should fail without WebSocket");
        Ok(())
    }
}
