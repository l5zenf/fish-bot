//! Echo plugin — built with #[plugin] + #[plugin_handlers] proc macros.
//!
//! Demonstrates both message and event handlers using the new plugin SDK API.

use fish_plugin_sdk::prelude::*;
use fish_plugin_sdk::{plugin, plugin_handlers};

#[plugin(
    id = "echo",
    name = "回声插件",
    description = "一个简单的回声插件，用于演示自动回复功能",
    version = "1.0.0",
    author = "Kaguya233qwq",
)]
#[derive(Default)]
pub struct EchoPlugin;

#[plugin_handlers]
impl EchoPlugin {
    #[command("/echo")]
    async fn echo(&self, ctx: Context) -> Result<()> {
        let content = ctx.text()?.trim().to_string();
        ctx.reply(format!("Echo: {}", content)).await
    }

    #[event("order_create")]
    async fn on_order(&self, ctx: Context) -> Result<()> {
        tracing::info!("order_create event: {:?}", ctx.payload()?);
        Ok(())
    }

    #[event("item_purchased")]
    async fn on_purchase(&self, ctx: Context) -> Result<()> {
        tracing::info!("item_purchased event: {:?}", ctx.payload()?);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fish_plugin_sdk::HandlerContext;
    use fish_plugin_sdk::MessageEvent;
    use fish_plugin_sdk::MessageChain;
    use fish_plugin_sdk::BaseAdapter;
    use fish_plugin_sdk::Ctx;
    use fish_plugin_sdk::Telemetry;
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::sync::RwLock;

    struct MockAdapter;
    #[async_trait::async_trait]
    impl BaseAdapter for MockAdapter {
        fn set_callback(&self, _: Box<dyn Fn(MessageEvent) + Send + Sync>) {}
        async fn send(&self, _: &str, _: &MessageChain, _: Option<&str>) -> Result<()> { Ok(()) }
        async fn run(&self) -> Result<()> { Ok(()) }
    }

    fn make_handler_context(event: MessageEvent) -> HandlerContext {
        HandlerContext {
            event,
            adapter: Arc::new(MockAdapter),
            app_ctx: Arc::new(Ctx::new()),
            telemetry: Arc::new(Telemetry::new()),
            plugin_state: Some(Arc::new(RwLock::new(EchoPlugin::default())) as Arc<dyn Any + Send + Sync>),
        }
    }

    #[test]
    fn echo_metadata() {
        let plugin = EchoPlugin;
        let meta = plugin.metadata();
        assert_eq!(meta.id, "echo");
        assert_eq!(meta.name, "回声插件");
        assert_eq!(meta.version, "1.0.0");
        assert_eq!(meta.author, "Kaguya233qwq");
    }

    #[test]
    fn echo_handler_count() {
        let plugin = EchoPlugin;
        assert_eq!(plugin.message_handlers().len(), 1);
    }

    #[test]
    fn echo_handler_rule_fullmatch() -> anyhow::Result<()> {
        let plugin = EchoPlugin;
        let handler = &plugin.message_handlers()[0];
        let rule = handler.rule.as_ref().ok_or_else(|| anyhow::anyhow!("rule should exist"))?;

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
    async fn echo_handler_responds() {
        let plugin = EchoPlugin;
        let handler = &plugin.message_handlers()[0];

        let captured = Arc::new(Mutex::new(String::new()));
        let mut event = MessageEvent::new(
            "cid".into(), "uid".into(), "name".into(),
            MessageChain::from("/echo"),
            serde_json::json!({}),
        );

        let captured_clone = Arc::clone(&captured);
        event.set_callback(move |seg: fish_plugin_sdk::MessageSegment| {
            let c = Arc::clone(&captured_clone);
            Box::pin(async move { *c.lock().unwrap() = seg.summary(); })
        });

        let cx = make_handler_context(event);
        (handler.func)(cx).await.unwrap();

        assert_eq!(*captured.lock().unwrap(), "Echo: /echo");
    }

    #[tokio::test]
    async fn echo_handler_whitespace_input() {
        let plugin = EchoPlugin;
        let handler = &plugin.message_handlers()[0];

        let captured = Arc::new(Mutex::new(String::new()));
        let mut event = MessageEvent::new(
            "cid".into(), "uid".into(), "name".into(),
            MessageChain::from("  hello  "),
            serde_json::json!({}),
        );

        let captured_clone = Arc::clone(&captured);
        event.set_callback(move |seg: fish_plugin_sdk::MessageSegment| {
            let c = Arc::clone(&captured_clone);
            Box::pin(async move { *c.lock().unwrap() = seg.summary(); })
        });

        let cx = make_handler_context(event);
        (handler.func)(cx).await.unwrap();

        assert_eq!(*captured.lock().unwrap(), "Echo: hello");
    }

    #[test]
    fn echo_event_handlers() {
        let plugin = EchoPlugin;
        let handlers = plugin.event_handlers();
        assert_eq!(handlers.len(), 2);
        assert!(handlers.contains_key("order_create"));
        assert!(handlers.contains_key("item_purchased"));
    }

    #[tokio::test]
    async fn echo_event_handler_type() {
        let plugin = EchoPlugin;
        let handlers = plugin.event_handlers();
        let order_handlers = handlers.get("order_create").unwrap();
        assert_eq!(order_handlers.len(), 1);

        let event = std::sync::Arc::new(fish_plugin_sdk::SystemEvent {
            event_type: "order_create".into(),
            payload: std::sync::Arc::new(serde_json::json!({"order_id": "123"})),
        });

        (order_handlers[0].func)(
            event,
            Arc::new(MockAdapter) as Arc<dyn BaseAdapter>,
            Arc::new(Ctx::new()),
            Some(Arc::new(RwLock::new(EchoPlugin::default())) as Arc<dyn Any + Send + Sync>),
        )
        .await
        .unwrap();
    }
}
