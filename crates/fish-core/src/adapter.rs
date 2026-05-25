use async_trait::async_trait;

use crate::error::Result;
use crate::event::{MessageEvent, SystemEvent};
use crate::message::MessageChain;

#[async_trait]
pub trait AdapterEventSink: Send + Sync {
    async fn handle_message(&self, event: MessageEvent) -> Result<()>;

    async fn handle_system(&self, _event: SystemEvent) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait BaseAdapter: Send + Sync {
    async fn send(&self, target_id: &str, message: &MessageChain, cid: Option<&str>) -> Result<()>;

    async fn run(&self, sink: std::sync::Arc<dyn AdapterEventSink>) -> Result<()>;
}
