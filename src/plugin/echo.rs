use crate::plugin::Plugin;
use crate::adapter::BaseAdapter;
use crate::model::{Message, MessageEvent};
use std::sync::Arc;

pub struct EchoPlugin;

impl Plugin for EchoPlugin {
    fn on_message(&self, event: &MessageEvent, _adapter: Arc<dyn BaseAdapter>) {
        if let Some(Message::Text { text }) = event.messages.first() {
            tracing::info!("EchoPlugin received text: {}", text);
        }
    }
}
