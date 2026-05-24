use std::collections::HashMap;
use std::sync::Arc;

use fish_core::message::{MessageChain, MessageSegment};
use crate::plugin::{EventHandler, HandlerContext, MessageHandler, Plugin, PluginMetadata};

/// Echo plugin — replies with the received message content.
/// Also demonstrates event handler pattern for business events.
pub struct EchoPlugin {
    metadata: PluginMetadata,
    handlers: Vec<MessageHandler>,
    event_handlers: HashMap<String, Vec<EventHandler>>,
}

impl EchoPlugin {
    pub fn new() -> Self {
        Self {
            metadata: PluginMetadata {
                id: "echo".into(),
                name: "回声插件".into(),
                description: "一个简单的回声插件，用于演示自动回复功能".into(),
                version: "1.0.0".into(),
                author: "Kaguya233qwq".into(),
            },
            handlers: vec![MessageHandler::exact(
                "echo",
                vec!["/echo"],
                Arc::new(|cx: HandlerContext| {
                    Box::pin(async move {
                        let content = cx.event.plain_text().trim().to_string();
                        let reply_msg = format!("Echo: {}", content);
                        cx.event.reply(MessageSegment::text(reply_msg)).await;
                        Ok(())
                    })
                }),
            )],
            event_handlers: {
                let mut map = HashMap::new();
                map.insert("order_create".into(), vec![
                    EventHandler::new("order_notify", Arc::new(|event, adapter, _ctx| {
                        Box::pin(async move {
                            // event.payload 包含原始业务数据
                            let payload = &event.payload;
                            tracing::info!("order_create event: {:?}", payload);
                            // 可以在这里发送通知、回复用户等
                            let _ = adapter.send("target_user", &MessageChain::from("感谢您的下单！"), None).await;
                        })
                    })),
                ]);
                map.insert("item_purchased".into(), vec![
                    EventHandler::new("purchase_notify", Arc::new(|event, adapter, _ctx| {
                        Box::pin(async move {
                            tracing::info!("item_purchased event: {:?}", event.payload);
                            let _ = adapter.send("target_user", &MessageChain::from("商品已售出！"), None).await;
                        })
                    })),
                ]);
                map
            },
        }
    }
}

impl Plugin for EchoPlugin {
    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }

    fn message_handlers(&self) -> &[MessageHandler] {
        &self.handlers
    }

    fn event_handlers(&self) -> HashMap<String, Vec<EventHandler>> {
        self.event_handlers.clone()
    }
}

impl Default for EchoPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use fish_adapter::adapter::BaseAdapter;
    use fish_core::ctx::Ctx;
    use fish_core::error::Result;
    use fish_core::event::MessageEvent;
    use fish_core::message::MessageChain;

    struct MockAdapter;
    #[async_trait]
    impl BaseAdapter for MockAdapter {
        fn set_callback(&self, _: Box<dyn Fn(MessageEvent) + Send + Sync>) {}
        async fn send(&self, _: &str, _: &MessageChain, _: Option<&str>) -> Result<()> { Ok(()) }
        async fn run(&self) -> Result<()> { Ok(()) }
    }

    #[test]
    fn t2_10_echo_metadata() {
        let plugin = EchoPlugin::new();
        let meta = plugin.metadata();
        assert_eq!(meta.id, "echo");
        assert_eq!(meta.name, "回声插件");
        assert_eq!(meta.version, "1.0.0");
    }

    #[test]
    fn t2_11_echo_handlers() {
        let plugin = EchoPlugin::new();
        let handlers = plugin.message_handlers();
        assert_eq!(handlers.len(), 1);
        assert!(handlers[0].rule.is_some());
    }

    #[tokio::test]
    async fn t2_12_echo_handler_matches() {
        let plugin = EchoPlugin::new();
        let handlers = plugin.message_handlers();
        let handler = &handlers[0];

        let captured = Arc::new(parking_lot::Mutex::new(String::new()));
        let mut event = MessageEvent::new(
            "cid".into(), "uid".into(), "name".into(),
            MessageChain::from("/echo"),
            serde_json::json!({}),
        );

        let captured_clone = Arc::clone(&captured);
        event.set_callback(move |seg: MessageSegment| {
            let c = Arc::clone(&captured_clone);
            Box::pin(async move { *c.lock() = seg.summary(); })
        });

        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());

        let _ = (handler.func)(HandlerContext { event, adapter, app_ctx: ctx, telemetry: Arc::new(fish_core::telemetry::Telemetry::new()) }).await;

        assert_eq!(*captured.lock(), "Echo: /echo");
    }

    #[test]
    fn t2_13_echo_rule_requires_fullmatch() -> anyhow::Result<()> {
        let plugin = EchoPlugin::new();
        let handlers = plugin.message_handlers();
        let rule = handlers[0].rule.as_ref().ok_or_else(|| anyhow::anyhow!("rule should exist"))?;

        let match_event = MessageEvent::new(
            "cid".into(), "uid".into(), "name".into(),
            MessageChain::from("/echo"),
            serde_json::json!({}),
        );
        assert!(rule.check(&match_event));

        let no_match = MessageEvent::new(
            "cid".into(), "uid".into(), "name".into(),
            MessageChain::from("/echo help"),
            serde_json::json!({}),
        );
        assert!(!rule.check(&no_match));
        Ok(())
    }

    #[tokio::test]
    async fn t2_24_handler_empty_input() -> anyhow::Result<()> {
        let plugin = EchoPlugin::new();
        let handlers = plugin.message_handlers();
        let handler = &handlers[0];

        let captured = Arc::new(parking_lot::Mutex::new(String::new()));
        let mut event = MessageEvent::new(
            "cid".into(), "uid".into(), "name".into(),
            MessageChain::from(""),
            serde_json::json!({}),
        );

        let captured_clone = Arc::clone(&captured);
        event.set_callback(move |seg: MessageSegment| {
            let c = Arc::clone(&captured_clone);
            Box::pin(async move { *c.lock() = seg.summary(); })
        });

        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());

        let _ = (handler.func)(HandlerContext { event, adapter, app_ctx: ctx, telemetry: Arc::new(fish_core::telemetry::Telemetry::new()) }).await;

        assert_eq!(*captured.lock(), "Echo: ");
        Ok(())
    }

    #[tokio::test]
    async fn t2_25_handler_whitespace_input() -> anyhow::Result<()> {
        let plugin = EchoPlugin::new();
        let handlers = plugin.message_handlers();
        let handler = &handlers[0];

        let captured = Arc::new(parking_lot::Mutex::new(String::new()));
        let mut event = MessageEvent::new(
            "cid".into(), "uid".into(), "name".into(),
            MessageChain::from("  hello  "),
            serde_json::json!({}),
        );

        let captured_clone = Arc::clone(&captured);
        event.set_callback(move |seg: MessageSegment| {
            let c = Arc::clone(&captured_clone);
            Box::pin(async move { *c.lock() = seg.summary(); })
        });

        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());

        let _ = (handler.func)(HandlerContext { event, adapter, app_ctx: ctx, telemetry: Arc::new(fish_core::telemetry::Telemetry::new()) }).await;

        assert_eq!(*captured.lock(), "Echo: hello");
        Ok(())
    }

    #[test]
    fn t2_28_echo_default() -> anyhow::Result<()> {
        let plugin = EchoPlugin::default();
        let meta = plugin.metadata();
        assert_eq!(meta.id, "echo");
        Ok(())
    }

    #[test]
    fn t2_29_echo_metadata_fields() -> anyhow::Result<()> {
        let plugin = EchoPlugin::new();
        let meta = plugin.metadata();
        assert_eq!(meta.author, "Kaguya233qwq");
        assert!(meta.description.contains("回声"));
        Ok(())
    }
}
