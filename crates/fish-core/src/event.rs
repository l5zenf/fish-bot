use crate::message::{MessageChain, MessageSegment};
use std::future::Future;
use std::pin::Pin;

type ReplyFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Message event context, matching Python event.py MessageEvent
#[derive(Clone)]
pub struct MessageEvent {
    pub cid: String,
    pub sender_id: String,
    pub sender_name: String,
    pub messages: MessageChain,
    pub raw_payload: serde_json::Value,
    callback_func: Option<std::sync::Arc<dyn Fn(MessageSegment) -> ReplyFuture + Send + Sync>>,
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
            raw_payload,
            callback_func: None,
        }
    }

    pub fn set_callback(
        &mut self,
        cb: impl Fn(MessageSegment) -> ReplyFuture + Send + Sync + 'static,
    ) {
        self.callback_func = Some(std::sync::Arc::new(cb));
    }

    /// Reply to this message event — sends a message back to the sender.
    pub async fn reply(&self, msg: impl Into<MessageSegment>) {
        if let Some(ref cb) = self.callback_func {
            cb(msg.into()).await;
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
    use crate::message::MessageSegment;

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
        assert_eq!(event.raw_payload, raw);
        assert!(event.callback_func.is_none());
    }

    #[tokio::test]
    async fn t1_15_set_callback_and_reply() {
        use std::sync::Arc;

        let callback_called = Arc::new(parking_lot::Mutex::new(String::new()));
        let mut event = MessageEvent::new(
            "cid1".into(),
            "uid1".into(),
            "Alice".into(),
            MessageChain::from("hello"),
            serde_json::json!({}),
        );

        let captured = Arc::clone(&callback_called);
        event.set_callback(move |seg: MessageSegment| {
            let c = Arc::clone(&captured);
            Box::pin(async move {
                let mut lock = c.lock();
                *lock = seg.summary();
            })
        });

        event.reply(MessageSegment::text("pong")).await;
        assert_eq!(*callback_called.lock(), "pong");
    }

    #[tokio::test]
    async fn t1_16_reply_without_callback_silent() {
        let event = MessageEvent::new(
            "cid1".into(),
            "uid1".into(),
            "Alice".into(),
            MessageChain::from("hello"),
            serde_json::json!({}),
        );

        // Should not panic or error
        event.reply(MessageSegment::text("pong")).await;
    }

    #[test]
    fn t1_17_event_delegates_to_chain() {
        let mut chain = MessageChain::new();
        chain.append(MessageSegment::text("hello"));
        chain.append(MessageSegment::image("pic.jpg"));

        let event = MessageEvent::new(
            "cid".into(), "uid".into(), "Alice".into(), chain, serde_json::json!({}),
        );

        assert_eq!(event.plain_text(), "hello");
        assert!(event.has_image());
        assert_eq!(event.summary(), "hello [图片]");
    }

    #[test]
    fn t1_18_debug_no_callback_info() {
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
        assert!(!debug_str.contains("callback_func"));
        assert!(!debug_str.contains("callback"));
    }
}
