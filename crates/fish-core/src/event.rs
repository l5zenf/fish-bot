use crate::message::MessageChain;
use std::sync::Arc;

/// A system-level event (non-chat-message) received from the WebSocket.
/// Examples: trade order placed, item purchased, system notifications.
#[derive(Clone, Debug)]
pub struct SystemEvent {
    pub event_type: String,
    pub payload: Arc<serde_json::Value>,
}

impl SystemEvent {
    pub fn new(event_type: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            event_type: event_type.into(),
            payload: Arc::new(payload),
        }
    }
}

/// Message event context.
#[derive(Clone)]
pub struct MessageEvent {
    pub cid: String,
    pub sender_id: String,
    pub sender_name: String,
    pub messages: MessageChain,
    pub raw_payload: Arc<serde_json::Value>,
}

impl MessageEvent {
    pub fn new(
        cid: String,
        sender_id: String,
        sender_name: String,
        messages: MessageChain,
        raw_payload: serde_json::Value,
    ) -> Self {
        Self {
            cid,
            sender_id,
            sender_name,
            messages,
            raw_payload: Arc::new(raw_payload),
        }
    }

    /// Get plain text from the message chain.
    pub fn plain_text(&self) -> String {
        self.messages.plain_text()
    }

    /// Check if the message contains an image.
    pub fn has_image(&self) -> bool {
        self.messages.has_image()
    }

    /// Get a human-readable summary of the message.
    pub fn summary(&self) -> String {
        self.messages.summary()
    }
}

impl std::fmt::Debug for MessageEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageEvent")
            .field("cid", &self.cid)
            .field("sender_id", &self.sender_id)
            .field("sender_name", &self.sender_name)
            .field("messages", &self.messages.summary())
            .field("raw_payload", &self.raw_payload)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t1_14_event_new_all_fields() {
        let chain = MessageChain::from("hello");
        let raw = serde_json::json!({"key": "val"});
        let event = MessageEvent::new(
            "cid1".into(),
            "uid1".into(),
            "Alice".into(),
            chain.clone(),
            raw.clone(),
        );

        assert_eq!(event.cid, "cid1");
        assert_eq!(event.sender_id, "uid1");
        assert_eq!(event.sender_name, "Alice");
        assert_eq!(event.messages.plain_text(), "hello");
        assert_eq!(*event.raw_payload, raw);
    }

    #[test]
    fn t1_15_event_delegates_to_chain() {
        let mut chain = MessageChain::new();
        chain.append(crate::message::MessageSegment::text("hello"));
        chain.append(crate::message::MessageSegment::image("pic.jpg"));

        let event = MessageEvent::new(
            "cid".into(),
            "uid".into(),
            "Alice".into(),
            chain,
            serde_json::json!({}),
        );

        assert_eq!(event.plain_text(), "hello");
        assert!(event.has_image());
        assert_eq!(event.summary(), "hello [图片]");
    }

    #[test]
    fn t1_16_debug_stays_data_only() {
        let event = MessageEvent::new(
            "cid".into(),
            "uid".into(),
            "Alice".into(),
            MessageChain::from("hello"),
            serde_json::json!({"key": "val"}),
        );

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("cid"));
        assert!(debug_str.contains("Alice"));
    }
}
