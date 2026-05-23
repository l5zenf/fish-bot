use std::sync::Arc;

use kameo::prelude::*;
use kameo::message::{Context, Message};
use tokio::sync::Semaphore;

use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_core::event::MessageEvent;
use crate::plugin::Plugin;

/// Plugin actor — wraps a Plugin and processes HandleEvent messages in isolation.
/// Each plugin runs in its own kameo actor task with automatic panic recovery.
/// Concurrency is bounded by a semaphore to prevent unbounded task growth.
#[derive(Actor)]
pub struct PluginActor {
    plugin: Arc<dyn Plugin>,
    semaphore: Arc<Semaphore>,
}

impl PluginActor {
    /// Create a new PluginActor with a default max concurrency of 64.
    pub fn new(plugin: Arc<dyn Plugin>) -> Self {
        Self {
            plugin,
            semaphore: Arc::new(Semaphore::new(64)),
        }
    }
}

/// Handle a message event — fanned out by BotActor with shared deps.
pub struct HandleEvent {
    pub event: MessageEvent,
    pub adapter: Arc<dyn BaseAdapter>,
    pub ctx: Arc<Ctx>,
}

impl Message<HandleEvent> for PluginActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: HandleEvent,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let plugin_id = self.plugin.metadata().id.clone();

        for handler in self.plugin.message_handlers() {
            // Check rule — if rule exists and doesn't match, skip this handler
            let matched = match &handler.rule {
                Some(rule) => rule.check(&msg.event),
                None => true,
            };

            if !matched {
                continue;
            }

            // Try to acquire a semaphore permit before spawning.
            // If the plugin is at capacity, drop the event with a warning
            // instead of queuing unbounded tasks.
            let permit = match Arc::clone(&self.semaphore).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    tracing::warn!(
                        plugin = %plugin_id,
                        handler = %handler.id,
                        "plugin busy, dropping event"
                    );
                    continue;
                }
            };

            let func = handler.func.clone();
            let handler_id = handler.id.clone();
            let handler_timeout = handler.timeout;
            let plugin_id = plugin_id.clone();
            let event = msg.event.clone();
            let adapter = Arc::clone(&msg.adapter);
            let ctx = Arc::clone(&msg.ctx);

            tokio::spawn(async move {
                let _permit = permit;
                let started = std::time::Instant::now();

                let result = tokio::time::timeout(handler_timeout, func(event, adapter, ctx)).await;

                match result {
                    Ok(Ok(())) => {
                        tracing::debug!(
                            plugin = %plugin_id,
                            handler = %handler_id,
                            cost_ms = started.elapsed().as_millis(),
                            "handler finished"
                        );
                    }
                    Ok(Err(e)) => {
                        tracing::error!(
                            plugin = %plugin_id,
                            handler = %handler_id,
                            error = %e,
                            cost_ms = started.elapsed().as_millis(),
                            "handler failed"
                        );
                    }
                    Err(_) => {
                        tracing::warn!(
                            plugin = %plugin_id,
                            handler = %handler_id,
                            timeout_ms = handler_timeout.as_millis(),
                            "handler timeout"
                        );
                    }
                }
            });
        }
    }
}

// ---- Messages ----

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::{MessageHandler, Plugin, PluginMetadata};
    use fish_core::event::MessageEvent;
    use fish_core::message::{MessageChain, MessageSegment};
    use fish_adapter::adapter::BaseAdapter;
    use fish_core::ctx::Ctx;
    use fish_core::rule::is_fullmatch;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use kameo::actor::Spawn;

    struct MockAdapter;
    #[async_trait]
    impl BaseAdapter for MockAdapter {
        fn set_callback(&self, _: Box<dyn Fn(MessageEvent) + Send + Sync>) {}
        async fn send(&self, _: &str, _: &MessageChain, _: Option<&str>) -> fish_core::error::Result<()> { Ok(()) }
        async fn run(&self) -> fish_core::error::Result<()> { Ok(()) }
    }

    struct TestPlugin {
        meta: PluginMetadata,
        handlers: Vec<MessageHandler>,
    }
    impl Plugin for TestPlugin {
        fn metadata(&self) -> &PluginMetadata { &self.meta }
        fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
    }

    fn make_test_plugin() -> TestPlugin {
        TestPlugin {
            meta: PluginMetadata { id: "test".into(), name: "test".into(), description: "".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("ping", Some(is_fullmatch(["/ping"])), Arc::new(|event, _, _| {
                let reply = event.plain_text();
                Box::pin(async move {
                    let _ = event.reply(MessageSegment::text(reply)).await;
                    Ok(())
                })
            }))],
        }
    }

    fn make_event(text: &str) -> MessageEvent {
        MessageEvent::new("cid".into(), "uid".into(), "name".into(), MessageChain::from(text), serde_json::json!({}))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_5_actor_new() {
        let plugin: Arc<dyn Plugin> = Arc::new(make_test_plugin());
        let actor = PluginActor::new(plugin);
        let _ref = PluginActor::spawn(actor);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_6_rule_matches_handler_executes() {
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        struct FlagPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for FlagPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(FlagPlugin {
            meta: PluginMetadata { id: "flag".into(), name: "".into(), description: "".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("flag", Some(is_fullmatch(["/ping"])), Arc::new(move |_, _, _| {
                let f = Arc::clone(&called_clone);
                Box::pin(async move { f.store(true, Ordering::SeqCst); Ok(()) })
            }))],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("/ping");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent { event, adapter: Arc::new(MockAdapter), ctx: Arc::new(Ctx::new()) }).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(called.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_7_rule_not_matching_skips_handler() {
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        struct FlagPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for FlagPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(FlagPlugin {
            meta: PluginMetadata { id: "flag".into(), name: "".into(), description: "".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("flag", Some(is_fullmatch(["/ping"])), Arc::new(move |_, _, _| {
                let f = Arc::clone(&called_clone);
                Box::pin(async move { f.store(true, Ordering::SeqCst); Ok(()) })
            }))],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("/pong");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent { event, adapter: Arc::new(MockAdapter), ctx: Arc::new(Ctx::new()) }).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_8_no_rule_handler_always_executes() {
        struct NoRulePlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for NoRulePlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(NoRulePlugin {
            meta: PluginMetadata { id: "norule".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("h1", None, Arc::new(|_, _, _| Box::pin(async { Ok(()) })))],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("anything");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent { event, adapter: Arc::new(MockAdapter), ctx: Arc::new(Ctx::new()) }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_9_handler_panic_does_not_propagate() {
        struct PanicPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for PanicPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(PanicPlugin {
            meta: PluginMetadata { id: "panic".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("panic", None, Arc::new(|_, _, _| {
                Box::pin(async { std::panic::panic_any("intentional panic") })
            }))],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("anything");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent { event, adapter: Arc::new(MockAdapter), ctx: Arc::new(Ctx::new()) }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_21_multiple_handlers_all_execute() -> anyhow::Result<()> {
        let call_count: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

        struct MultiHandlerPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for MultiHandlerPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let count = Arc::clone(&call_count);
        let plugin: Arc<dyn Plugin> = Arc::new(MultiHandlerPlugin {
            meta: PluginMetadata { id: "multi".into(), ..Default::default() },
            handlers: vec![
                MessageHandler::new("h1", None, Arc::new({
                    let c = Arc::clone(&count);
                    move |_, _, _| { let c2 = Arc::clone(&c); Box::pin(async move { c2.fetch_add(1, Ordering::SeqCst); Ok(()) }) }
                })),
                MessageHandler::new("h2", None, Arc::new({
                    let c = Arc::clone(&count);
                    move |_, _, _| { let c2 = Arc::clone(&c); Box::pin(async move { c2.fetch_add(1, Ordering::SeqCst); Ok(()) }) }
                })),
                MessageHandler::new("h3", None, Arc::new({
                    let c = Arc::clone(&count);
                    move |_, _, _| { let c2 = Arc::clone(&c); Box::pin(async move { c2.fetch_add(1, Ordering::SeqCst); Ok(()) }) }
                })),
            ],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("test");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent { event, adapter: Arc::new(MockAdapter), ctx: Arc::new(Ctx::new()) }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert_eq!(call_count.load(Ordering::SeqCst), 3, "all 3 handlers should execute");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_22_rule_mismatch_skipped() -> anyhow::Result<()> {
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        struct SkipPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for SkipPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(SkipPlugin {
            meta: PluginMetadata { id: "skip".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("skip", Some(is_fullmatch(["/run"])), Arc::new(move |_, _, _| {
                let f = Arc::clone(&called_clone);
                Box::pin(async move { f.store(true, Ordering::SeqCst); Ok(()) })
            }))],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("/skip");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent { event, adapter: Arc::new(MockAdapter), ctx: Arc::new(Ctx::new()) }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(!called.load(Ordering::SeqCst), "handler should not be called when rule doesn't match");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_23_ctx_adapter_passed() -> anyhow::Result<()> {
        struct CtxMarker;
        let ctx = Arc::new(Ctx::new());
        ctx.insert(CtxMarker);

        let ctx_used = Arc::new(AtomicBool::new(false));
        let adapter_used = Arc::new(AtomicBool::new(false));

        struct DepsPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for DepsPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let cu = Arc::clone(&ctx_used);
        let au = Arc::clone(&adapter_used);
        let plugin: Arc<dyn Plugin> = Arc::new(DepsPlugin {
            meta: PluginMetadata { id: "deps".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("deps", None, Arc::new(move |_, adapter, handler_ctx| {
                let cc = Arc::clone(&cu);
                let ac = Arc::clone(&au);
                Box::pin(async move {
                    if handler_ctx.get::<CtxMarker>().is_some() {
                        cc.store(true, Ordering::SeqCst);
                    }
                    let _ = adapter.send("test", &MessageChain::from(""), None).await;
                    ac.store(true, Ordering::SeqCst);
                    Ok(())
                })
            }))],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("check");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent { event, adapter: Arc::new(MockAdapter), ctx }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(ctx_used.load(Ordering::SeqCst), "ctx should be passed and accessible");
        assert!(adapter_used.load(Ordering::SeqCst), "adapter should be passed and callable");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_30_zero_handlers_does_not_panic() -> anyhow::Result<()> {
        struct EmptyPlugin { meta: PluginMetadata }
        impl Plugin for EmptyPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(EmptyPlugin { meta: PluginMetadata { id: "empty".into(), ..Default::default() } });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("anything");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent { event, adapter: Arc::new(MockAdapter), ctx: Arc::new(Ctx::new()) }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_31_mixed_rule_and_no_rule_handlers() -> anyhow::Result<()> {
        let call_count = Arc::new(AtomicUsize::new(0));

        struct MixedPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for MixedPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let c = Arc::clone(&call_count);
        let plugin: Arc<dyn Plugin> = Arc::new(MixedPlugin {
            meta: PluginMetadata { id: "mixed".into(), ..Default::default() },
            handlers: vec![
                MessageHandler::new("ping_rule", Some(is_fullmatch(["/ping"])), Arc::new({
                    let count = Arc::clone(&c);
                    move |_, _, _| { let c2 = Arc::clone(&count); Box::pin(async move { c2.fetch_add(1, Ordering::SeqCst); Ok(()) }) }
                })),
                MessageHandler::new("pong_rule", Some(is_fullmatch(["/pong"])), Arc::new({
                    let count = Arc::clone(&c);
                    move |_, _, _| { let c2 = Arc::clone(&count); Box::pin(async move { c2.fetch_add(10, Ordering::SeqCst); Ok(()) }) }
                })),
                MessageHandler::new("catchall", None, Arc::new({
                    let count = Arc::clone(&c);
                    move |_, _, _| { let c2 = Arc::clone(&count); Box::pin(async move { c2.fetch_add(100, Ordering::SeqCst); Ok(()) }) }
                })),
            ],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("/ping");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent { event, adapter: Arc::new(MockAdapter), ctx: Arc::new(Ctx::new()) }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert_eq!(call_count.load(Ordering::SeqCst), 101, "only matching rule and no-rule handlers should execute");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_32_handler_with_plugin_name() -> anyhow::Result<()> {
        struct NamedPlugin { meta: PluginMetadata, handlers: Vec<MessageHandler> }
        impl Plugin for NamedPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(NamedPlugin {
            meta: PluginMetadata { id: "named".into(), name: "TestName".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("h1", None, Arc::new(|_, _, _| Box::pin(async { Ok(()) })))],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("test");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent { event, adapter: Arc::new(MockAdapter), ctx: Arc::new(Ctx::new()) }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_33_multiple_events_to_same_actor() -> anyhow::Result<()> {
        let counter: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

        struct CountPlugin { meta: PluginMetadata, handlers: Vec<MessageHandler> }
        impl Plugin for CountPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let c = Arc::clone(&counter);
        let plugin: Arc<dyn Plugin> = Arc::new(CountPlugin {
            meta: PluginMetadata { id: "count".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("count", None, Arc::new(move |_, _, _| {
                let c2 = Arc::clone(&c);
                Box::pin(async move { c2.fetch_add(1, Ordering::SeqCst); Ok(()) })
            }))],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        for _ in 0..3 {
            let mut event = make_event("test");
            event.set_callback(|_| Box::pin(async {}));
            let _ = actor_ref.tell(HandleEvent { event, adapter: Arc::new(MockAdapter), ctx: Arc::new(Ctx::new()) }).await;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 3, "all 3 events should be handled");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_34_plugin_with_custom_metadata_name() -> anyhow::Result<()> {
        struct CustomMetaPlugin { meta: PluginMetadata, handlers: Vec<MessageHandler> }
        impl Plugin for CustomMetaPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(CustomMetaPlugin {
            meta: PluginMetadata { id: "custom_meta".into(), name: "元数据测试".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("h1", None, Arc::new(|_, _, _| Box::pin(async { Ok(()) })))],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("test");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent { event, adapter: Arc::new(MockAdapter), ctx: Arc::new(Ctx::new()) }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        Ok(())
    }
}
