use std::sync::Arc;

use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_core::event::MessageEvent;
use fish_core::message::MessageSegment;
use crate::plugin::{MessageHandler, Plugin, PluginMetadata};
use fish_core::rule::is_fullmatch;

/// Echo plugin — replies with the received message content.
pub struct EchoPlugin;

impl EchoPlugin {
    pub fn new() -> Self {
        Self
    }
}

impl Plugin for EchoPlugin {
    fn metadata(&self) -> PluginMetadata {
        PluginMetadata {
            id: "echo".into(),
            name: "回声插件".into(),
            description: "一个简单的回声插件，用于演示自动回复功能".into(),
            version: "1.0.0".into(),
            author: "Kaguya233qwq".into(),
        }
    }

    fn message_handlers(&self) -> Vec<MessageHandler> {
        vec![MessageHandler {
            func: Arc::new(
                |event: MessageEvent, _adapter: Arc<dyn BaseAdapter>, _ctx: Arc<Ctx>| {
                    Box::pin(async move {
                        let content = event.plain_text().trim().to_string();
                        let reply_msg = format!("Echo: {}", content);
                        let _ = event.reply(MessageSegment::text(reply_msg)).await;
                    })
                },
            ),
            rule: Some(is_fullmatch(["/echo"])),
        }]
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

        (handler.func)(event, adapter, ctx).await;

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

        (handler.func)(event, adapter, ctx).await;

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

        (handler.func)(event, adapter, ctx).await;

        assert_eq!(*captured.lock(), "Echo: hello");
        Ok(())
    }
}
