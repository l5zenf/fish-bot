use crate::plugin::Plugin;
use crate::adapter::BaseAdapter;
use crate::model::{Message, MessageEvent};
use std::sync::Arc;
use async_trait::async_trait;

pub struct EchoPlugin;

#[async_trait]
impl Plugin for EchoPlugin {
    async fn on_message(&self, event: &MessageEvent, _adapter: Arc<dyn BaseAdapter>) {
        if let Some(Message::Text { text }) = event.messages.first() {
            tracing::info!("EchoPlugin received text: {}", text);
        }
    }
}
