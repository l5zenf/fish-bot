use async_trait::async_trait;

use fish_core::AdapterEventSink;
use fish_core::BaseAdapter;
use fish_core::error::Result;
use fish_core::event::{MessageEvent, SystemEvent};
use fish_core::message::MessageChain;
#[derive(Default)]
pub struct LocalAdapter;

#[async_trait]
impl BaseAdapter for LocalAdapter {
    async fn send(&self, target_id: &str, message: &MessageChain, cid: Option<&str>) -> Result<()> {
        println!(
            "send -> target={target_id}, cid={}, payload={}",
            cid.unwrap_or(target_id),
            message.summary()
        );
        Ok(())
    }

    async fn run(&self, sink: std::sync::Arc<dyn AdapterEventSink>) -> Result<()> {
        sink.handle_message(MessageEvent::new(
            "demo-cid".into(),
            "demo-user".into(),
            "Demo User".into(),
            "/ping".into(),
            serde_json::json!({
                "source": "quickstart",
            }),
        ))
        .await?;

        sink.handle_message(MessageEvent::new(
            "demo-cid".into(),
            "demo-user".into(),
            "Demo User".into(),
            "hello fish runtime".into(),
            serde_json::json!({
                "source": "quickstart",
            }),
        ))
        .await?;

        sink.handle_system(SystemEvent::new(
            "quickstart_ready",
            serde_json::json!({"source": "quickstart"}),
        ))
        .await?;

        Ok(())
    }
}
