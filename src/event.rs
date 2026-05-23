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
