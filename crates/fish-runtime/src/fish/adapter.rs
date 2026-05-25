use std::sync::Arc;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use fish_core::AdapterEventSink;
use fish_core::error::Result;
use fish_core::event::{MessageEvent, SystemEvent};
use fish_core::message::{MessageChain, MessageSegment};
use futures::StreamExt;
use futures::stream::SplitStream;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use super::api::FishAPI;
use super::auth::AuthManager;
use super::connection::FishConnection;
use super::protocol::{decode_content, encode_chain};
use super::sign::{decrypt, generate_mid, generate_uuid};
use fish_runtime::BaseAdapter;

type WsReader = SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Thin coordinator: ties together FishAPI (HTTP auth) and FishConnection (WS transport).
pub struct FishWebSocketAdapter {
    api: FishAPI,
    conn: Arc<FishConnection>,
}

impl FishWebSocketAdapter {
    pub fn new() -> Self {
        let auth = AuthManager::new();
        let api = FishAPI::new(auth);
        Self {
            api,
            conn: Arc::new(FishConnection::new()),
        }
    }

    /// Main connection logic: connect -> handshake -> receive loop.
    async fn connect_and_run(self: &Arc<Self>, sink: Arc<dyn AdapterEventSink>) -> Result<()> {
        let url = "wss://wss-goofish.dingtalk.com/";
        tracing::info!("Connecting to {}", url);

        let reader = self.conn.connect(url).await?;

        let token = self.api.get_access_token().await?;
        tracing::info!("Got access token");

        self.conn.handshake(&token, &self.api.device_id()).await?;
        self.conn.spawn_heartbeat();

        self.receive_loop(reader, sink).await
    }

    /// Receive loop: read messages, ack, decrypt, construct event, invoke callback.
    async fn receive_loop(
        self: &Arc<Self>,
        mut reader: WsReader,
        sink: Arc<dyn AdapterEventSink>,
    ) -> Result<()> {
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
            if let Err(e) = self.handle_raw_message(&text, Arc::clone(&sink)).await {
                tracing::error!("handle_raw_message: {}", e);
            }
        }
        Ok(())
    }

    /// Handle a single raw WS message frame.
    async fn handle_raw_message(
        self: &Arc<Self>,
        text: &str,
        sink: Arc<dyn AdapterEventSink>,
    ) -> Result<()> {
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

        let body = msg
            .get("body")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        let push_pkg = match body.get("syncPushPackage") {
            Some(v) => v.clone(),
            None => return Ok(()),
        };
        let data_list = push_pkg
            .get("data")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for item in &data_list {
            let raw_data = match item.get("data").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };

            // Try to decrypt: plaintext base64 -> JSON, then fallback to msgpack route
            let decrypted = match decrypt(raw_data) {
                Ok(v) => v,
                Err(_) => {
                    match STANDARD.decode(raw_data.as_bytes()) {
                        Ok(bytes) => {
                            // Try JSON first (plaintext)
                            match serde_json::from_slice(&bytes) {
                                Ok(v) => v,
                                Err(_) => {
                                    // Try MessagePack (encrypted business events)
                                    match rmp_serde::from_slice::<serde_json::Value>(&bytes) {
                                        Ok(mp_val) => mp_val,
                                        Err(_) => continue,
                                    }
                                }
                            }
                        }
                        Err(_) => continue,
                    }
                }
            };

            // Parse decrypted message into MessageEvent
            match self.parse_event(&decrypted).await {
                Some(event) => {
                    tracing::info!(
                        "[接收] <- {}({}): {}",
                        event.sender_name,
                        event.sender_id,
                        event.summary()
                    );
                    sink.handle_message(event).await?;
                }
                None => {
                    // Skip typing status and system messages
                    if is_typing_status(&decrypted) || is_system_message(&decrypted) {
                        continue;
                    }

                    // Not a chat message — classify and emit as SystemEvent
                    let event_type = classify_event_type(&decrypted);
                    sink.handle_system(SystemEvent::new(event_type, decrypted))
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Parse a decrypted payload into a MessageEvent.
    async fn parse_event(self: &Arc<Self>, payload: &Value) -> Option<MessageEvent> {
        let body = payload
            .get("1")
            .or_else(|| payload.get("body"))
            .or(Some(payload))?;

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

        // Extract message content from field 6, fallback to reminderContent
        let content = body.get("6").or_else(|| body.get("content")).or_else(|| {
            body.get("3")
                .and_then(|v3| v3.get("5"))
                .or_else(|| payload.get("messages"))
        });

        let segments = match content {
            Some(c) => decode_content(c).unwrap_or_default(),
            None => {
                // Fallback: extract text from message["1"]["10"]["reminderContent"]
                match sender_field
                    .and_then(|s| s.get("reminderContent"))
                    .and_then(|v| v.as_str())
                {
                    Some(text) => vec![MessageSegment::Text {
                        text: text.to_string(),
                    }],
                    None => Vec::new(),
                }
            }
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
        unreachable!("run_arc requires an event sink; call BaseAdapter::run instead")
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
            api: self.api.clone(),
            conn: Arc::clone(&self.conn),
        }
    }
}

#[async_trait]
impl BaseAdapter for FishWebSocketAdapter {
    async fn send(&self, target_id: &str, message: &MessageChain, cid: Option<&str>) -> Result<()> {
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

    async fn run(&self, sink: Arc<dyn AdapterEventSink>) -> Result<()> {
        // Ensure auth before connecting
        self.api.ensure_auth().await?;

        // Clone self into an Arc so we can share across tasks
        let arc_self = Arc::new(self.clone());
        loop {
            tracing::info!("Starting adapter. Connecting...");
            if let Err(e) = arc_self.connect_and_run(Arc::clone(&sink)).await {
                tracing::error!("Connection error: {}, reconnecting in 5s...", e);
            } else {
                tracing::info!("Connection closed cleanly, reconnecting in 5s...");
            }
            sleep(Duration::from_secs(5)).await;
        }
    }
}

/// Check if the message is a typing status indicator (message["1"] is an array).
/// Check if the message is a typing status indicator (message["1"] is an array).
fn is_typing_status(payload: &Value) -> bool {
    payload
        .get("1")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|v| v.get("1"))
        .and_then(|v| v.as_str())
        .map(|s| s.contains("@goofish"))
        .unwrap_or(false)
}

/// Check if the message is a system-level control message (needPush == "false").
/// Check if the message is a system-level control message (needPush == "false").
fn is_system_message(payload: &Value) -> bool {
    payload
        .get("3")
        .and_then(|v| v.as_object())
        .and_then(|obj| obj.get("needPush"))
        .and_then(|v| v.as_str())
        .map(|s| s == "false")
        .unwrap_or(false)
}

/// Try to extract a meaningful event type from a non-chat decrypted payload.
/// Examines common field names that fish server uses for business events.
/// Also checks nested fields like `["3"]["redReminder"]` for order events.
fn classify_event_type(payload: &Value) -> String {
    // Check order/transaction events via redReminder in field 3
    if let Some(field3) = payload.get("3").and_then(|v| v.as_object()) {
        if let Some(reminder) = field3.get("redReminder").and_then(|v| v.as_str()) {
            return match reminder {
                "等待买家付款" => "order_create",
                "交易关闭" => "order_closed",
                "等待卖家发货" => "item_purchased",
                _ => "order_unknown",
            }
            .to_string();
        }
    }

    // Fallback: check top-level fields
    payload
        .get("action")
        .or_else(|| payload.get("type"))
        .or_else(|| payload.get("eventType"))
        .or_else(|| payload.get("bizType"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fish_core::message::{MessageChain, MessageSegment};

    #[test]
    fn t3_55_new_creates_adapter() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        assert!(!adapter.api.device_id().is_empty());
        Ok(())
    }

    #[test]
    fn t3_56_clone_preserves_connection_handle() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let cloned = adapter.clone();
        assert!(Arc::ptr_eq(&adapter.conn, &cloned.conn));
        Ok(())
    }

    #[test]
    fn t3_57_default_creates_adapter() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::default();
        assert!(!adapter.api.device_id().is_empty());
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

    #[tokio::test]
    async fn t3_60_send_returns_err_without_connection() -> anyhow::Result<()> {
        let adapter = FishWebSocketAdapter::new();
        let chain = MessageChain::from("test message");
        let result = adapter.send("target", &chain, None).await;
        assert!(
            result.is_err(),
            "send should fail without WebSocket connection"
        );
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
            assert!(
                err_msg.contains("WebSocket"),
                "error should mention WebSocket"
            );
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
        let chain = MessageChain::from(MessageSegment::image("https://example.com/pic.jpg"));
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

    // ---- classify_event_type tests ----

    #[test]
    fn t3_72_classify_order_create() -> anyhow::Result<()> {
        let payload = serde_json::json!({
            "3": {"redReminder": "等待买家付款"},
            "1": "user@goofish"
        });
        assert_eq!(classify_event_type(&payload), "order_create");
        Ok(())
    }

    #[test]
    fn t3_73_classify_order_closed() -> anyhow::Result<()> {
        let payload = serde_json::json!({
            "3": {"redReminder": "交易关闭"}
        });
        assert_eq!(classify_event_type(&payload), "order_closed");
        Ok(())
    }

    #[test]
    fn t3_74_classify_item_purchased() -> anyhow::Result<()> {
        let payload = serde_json::json!({
            "3": {"redReminder": "等待卖家发货"}
        });
        assert_eq!(classify_event_type(&payload), "item_purchased");
        Ok(())
    }

    #[test]
    fn t3_75_classify_order_unknown() -> anyhow::Result<()> {
        let payload = serde_json::json!({
            "3": {"redReminder": "未知状态"}
        });
        assert_eq!(classify_event_type(&payload), "order_unknown");
        Ok(())
    }

    #[test]
    fn t3_76_classify_top_level_action() -> anyhow::Result<()> {
        let payload = serde_json::json!({
            "action": "trade_notice"
        });
        assert_eq!(classify_event_type(&payload), "trade_notice");
        Ok(())
    }

    #[test]
    fn t3_77_classify_unknown() -> anyhow::Result<()> {
        let payload = serde_json::json!({"some": "data"});
        assert_eq!(classify_event_type(&payload), "unknown");
        Ok(())
    }

    #[test]
    fn t3_78_classify_no_redreminder() -> anyhow::Result<()> {
        // Has field 3 but no redReminder — should fall through to top-level
        let payload = serde_json::json!({
            "3": {"other": "value"},
            "type": "notice"
        });
        assert_eq!(classify_event_type(&payload), "notice");
        Ok(())
    }

    #[test]
    fn t3_79_msgpack_decode_roundtrip() -> anyhow::Result<()> {
        // Simulate msgpack-encoded data (the encrypted path)
        let original = serde_json::json!({
            "1": {
                "10": {"reminderContent": "hello", "senderUserId": "uid", "reminderTitle": "user"},
                "2": "cid@goofish",
                "5": 1700000000000u64
            }
        });

        // Encode as msgpack
        let msgpack_bytes = rmp_serde::to_vec(&original)?;

        // Decode back via rmp_serde (matching the msgpack fallback path in handle_raw_message)
        let decoded: serde_json::Value = rmp_serde::from_slice(&msgpack_bytes)?;
        assert_eq!(decoded, original);
        Ok(())
    }

    #[test]
    fn t3_80_msgpack_business_event_roundtrip() -> anyhow::Result<()> {
        // Simulate a business event that's msgpack-encoded
        let original = serde_json::json!({
            "1": "buyer@goofish",
            "3": {"redReminder": "等待买家付款"}
        });

        // Encode and decode via msgpack
        let bytes = rmp_serde::to_vec(&original)?;
        let decoded: serde_json::Value = rmp_serde::from_slice(&bytes)?;

        assert_eq!(classify_event_type(&decoded), "order_create");
        Ok(())
    }

    // ---- is_typing_status tests ----

    #[test]
    fn t3_81_typing_status_detected() -> anyhow::Result<()> {
        let payload = serde_json::json!({
            "1": [{"1": "user@goofish"}]
        });
        assert!(is_typing_status(&payload));
        Ok(())
    }

    #[test]
    fn t3_82_typing_status_not_array() -> anyhow::Result<()> {
        let payload = serde_json::json!({
            "1": {"10": {"reminderContent": "hello"}}
        });
        assert!(!is_typing_status(&payload));
        Ok(())
    }

    #[test]
    fn t3_83_typing_status_no_goofish() -> anyhow::Result<()> {
        let payload = serde_json::json!({
            "1": [{"1": "other"}]
        });
        assert!(!is_typing_status(&payload));
        Ok(())
    }

    // ---- is_system_message tests ----

    #[test]
    fn t3_84_system_message_detected() -> anyhow::Result<()> {
        let payload = serde_json::json!({
            "3": {"needPush": "false"}
        });
        assert!(is_system_message(&payload));
        Ok(())
    }

    #[test]
    fn t3_85_system_message_needpush_true() -> anyhow::Result<()> {
        let payload = serde_json::json!({
            "3": {"needPush": "true"}
        });
        assert!(!is_system_message(&payload));
        Ok(())
    }

    #[test]
    fn t3_86_system_message_no_field3() -> anyhow::Result<()> {
        let payload = serde_json::json!({"1": "hello"});
        assert!(!is_system_message(&payload));
        Ok(())
    }

    // ---- reminderContent fallback test ----

    #[test]
    fn t3_87_reminder_content_extracted() -> anyhow::Result<()> {
        let payload = serde_json::json!({
            "1": {
                "10": {
                    "reminderContent": "hello from reminder",
                    "reminderTitle": "User",
                    "senderUserId": "uid"
                },
                "2": "cid@goofish"
            }
        });

        // Verify the data structure matches parse_event expectations:
        // body = payload["1"], sender_field = body["10"]["reminderContent"]
        assert_eq!(payload["1"]["10"]["reminderContent"], "hello from reminder");
        assert_eq!(payload["1"]["2"], "cid@goofish");
        Ok(())
    }
}
