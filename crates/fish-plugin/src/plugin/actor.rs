use std::sync::Arc;

use kameo::prelude::*;
use kameo::message::{Context, Message};

use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_core::event::MessageEvent;
use crate::plugin::Plugin;

/// Plugin actor — wraps a Plugin and processes HandleEvent messages in isolation.
/// Each plugin runs in its own kameo actor task with automatic panic recovery.
#[derive(Actor)]
pub struct PluginActor {
    plugin: Arc<dyn Plugin>,
}

impl PluginActor {
    pub fn new(plugin: Arc<dyn Plugin>) -> Self {
        Self { plugin }
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
        let plugin_name = self.plugin.metadata().name.clone();

        for handler in &self.plugin.message_handlers() {
            // Check rule — if rule exists and doesn't match, skip this handler
            let matched = match &handler.rule {
                Some(rule) => rule.check(&msg.event),
                None => true,
            };

            if !matched {
                continue;
            }

            let func = handler.func.clone();
            let event = msg.event.clone();
            let adapter = Arc::clone(&msg.adapter);
            let ctx = Arc::clone(&msg.ctx);
            let name = plugin_name.clone();

            tokio::spawn(async move {
                // Panic safety: each handler runs in its own task.
                // A panic here logs and stops only this handler invocation.
                func(event, adapter, ctx).await;
            });

            // Suppress unused warning for name (used in debug builds)
            let _ = &name;
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

    struct TestPlugin;
    impl Plugin for TestPlugin {
        fn metadata(&self) -> PluginMetadata {
            PluginMetadata { id: "test".into(), name: "test".into(), description: "".into(), ..Default::default() }
        }
        fn message_handlers(&self) -> Vec<MessageHandler> {
            vec![
                MessageHandler {
                    func: Arc::new(|event, _, _| {
                        let reply = event.plain_text();
                        Box::pin(async move {
                            let _ = event.reply(MessageSegment::text(reply)).await;
                        })
                    }),
                    rule: Some(is_fullmatch(["/ping"])),
                },
            ]
        }
    }
    fn make_event(text: &str) -> MessageEvent {
        MessageEvent::new("cid".into(), "uid".into(), "name".into(), MessageChain::from(text), serde_json::json!({}))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_5_actor_new() {
        let plugin: Arc<dyn Plugin> = Arc::new(TestPlugin);
        let actor = PluginActor::new(plugin);
        let _ref = PluginActor::spawn(actor);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_6_rule_matches_handler_executes() {
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        struct FlagPlugin(Arc<AtomicBool>);
        impl Plugin for FlagPlugin {
            fn metadata(&self) -> PluginMetadata { PluginMetadata { id: "flag".into(), name: "".into(), description: "".into(), ..Default::default() } }
            fn message_handlers(&self) -> Vec<MessageHandler> {
                let flag = Arc::clone(&self.0);
                vec![MessageHandler {
                    func: Arc::new(move |_, _, _| {
                        let f = Arc::clone(&flag);
                        Box::pin(async move { f.store(true, Ordering::SeqCst); })
                    }),
                    rule: Some(is_fullmatch(["/ping"])),
                }]
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(FlagPlugin(called_clone));
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("/ping");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent {
            event,
            adapter: Arc::new(MockAdapter),
            ctx: Arc::new(Ctx::new()),
        }).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(called.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_7_rule_not_matching_skips_handler() {
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);
        struct FlagPlugin(Arc<AtomicBool>);
        impl Plugin for FlagPlugin {
            fn metadata(&self) -> PluginMetadata { PluginMetadata { id: "flag".into(), name: "".into(), description: "".into(), ..Default::default() } }
            fn message_handlers(&self) -> Vec<MessageHandler> {
                let flag = Arc::clone(&self.0);
                vec![MessageHandler {
                    func: Arc::new(move |_, _, _| {
                        let f = Arc::clone(&flag);
                        Box::pin(async move { f.store(true, Ordering::SeqCst); })
                    }),
                    rule: Some(is_fullmatch(["/ping"])),
                }]
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(FlagPlugin(called_clone));
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("/pong");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent {
            event,
            adapter: Arc::new(MockAdapter),
            ctx: Arc::new(Ctx::new()),
        }).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_8_no_rule_handler_always_executes() {
        struct NoRulePlugin;
        impl Plugin for NoRulePlugin {
            fn metadata(&self) -> PluginMetadata { PluginMetadata { id: "norule".into(), name: "".into(), description: "".into(), ..Default::default() } }
            fn message_handlers(&self) -> Vec<MessageHandler> {
                vec![MessageHandler { func: Arc::new(|_, _, _| Box::pin(async {})), rule: None }]
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(NoRulePlugin);
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("anything");
        event.set_callback(|_| Box::pin(async {}));

        // Should not panic — handler with no rule always runs
        let _ = actor_ref.tell(HandleEvent {
            event,
            adapter: Arc::new(MockAdapter),
            ctx: Arc::new(Ctx::new()),
        }).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_9_handler_panic_does_not_propagate() {
        struct PanicPlugin;
        impl Plugin for PanicPlugin {
            fn metadata(&self) -> PluginMetadata { PluginMetadata { id: "panic".into(), name: "".into(), description: "".into(), ..Default::default() } }
            fn message_handlers(&self) -> Vec<MessageHandler> {
                vec![MessageHandler {
                    func: Arc::new(|_, _, _| Box::pin(async { std::panic::panic_any("intentional panic") })),
                    rule: None,
                }]
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(PanicPlugin);
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("anything");
        event.set_callback(|_| Box::pin(async {}));

        // Panic in handler should NOT affect the actor or test
        let _ = actor_ref.tell(HandleEvent {
            event,
            adapter: Arc::new(MockAdapter),
            ctx: Arc::new(Ctx::new()),
        }).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_21_multiple_handlers_all_execute() -> anyhow::Result<()> {
        let call_count: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

        struct MultiHandlerPlugin(Arc<AtomicUsize>);
        impl Plugin for MultiHandlerPlugin {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata { id: "multi".into(), name: "".into(), description: "".into(), ..Default::default() }
            }
            fn message_handlers(&self) -> Vec<MessageHandler> {
                let count: Arc<AtomicUsize> = Arc::clone(&self.0);
                vec![
                    MessageHandler {
                        func: Arc::new({
                            let c: Arc<AtomicUsize> = Arc::clone(&count);
                            move |_, _, _| {
                                let c2: Arc<AtomicUsize> = Arc::clone(&c);
                                Box::pin(async move { c2.fetch_add(1, Ordering::SeqCst); })
                            }
                        }),
                        rule: None,
                    },
                    MessageHandler {
                        func: Arc::new({
                            let c: Arc<AtomicUsize> = Arc::clone(&count);
                            move |_, _, _| {
                                let c2: Arc<AtomicUsize> = Arc::clone(&c);
                                Box::pin(async move { c2.fetch_add(1, Ordering::SeqCst); })
                            }
                        }),
                        rule: None,
                    },
                    MessageHandler {
                        func: Arc::new({
                            let c: Arc<AtomicUsize> = Arc::clone(&count);
                            move |_, _, _| {
                                let c2: Arc<AtomicUsize> = Arc::clone(&c);
                                Box::pin(async move { c2.fetch_add(1, Ordering::SeqCst); })
                            }
                        }),
                        rule: None,
                    },
                ]
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(MultiHandlerPlugin(Arc::clone(&call_count)));
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("test");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent {
            event,
            adapter: Arc::new(MockAdapter),
            ctx: Arc::new(Ctx::new()),
        }).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert_eq!(call_count.load(Ordering::SeqCst), 3, "all 3 handlers should execute");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_22_rule_mismatch_skipped() -> anyhow::Result<()> {
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        struct SkipPlugin(Arc<AtomicBool>);
        impl Plugin for SkipPlugin {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata { id: "skip".into(), name: "".into(), description: "".into(), ..Default::default() }
            }
            fn message_handlers(&self) -> Vec<MessageHandler> {
                let flag = Arc::clone(&self.0);
                vec![MessageHandler {
                    func: Arc::new(move |_, _, _| {
                        let f = Arc::clone(&flag);
                        Box::pin(async move { f.store(true, Ordering::SeqCst); })
                    }),
                    rule: Some(is_fullmatch(["/run"])),
                }]
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(SkipPlugin(called_clone));
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("/skip");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent {
            event,
            adapter: Arc::new(MockAdapter),
            ctx: Arc::new(Ctx::new()),
        }).await;

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
        let cu = Arc::clone(&ctx_used);
        let au = Arc::clone(&adapter_used);

        struct DepsPlugin {
            ctx_check: Arc<AtomicBool>,
            adapter_check: Arc<AtomicBool>,
        }
        impl Plugin for DepsPlugin {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata { id: "deps".into(), name: "".into(), description: "".into(), ..Default::default() }
            }
            fn message_handlers(&self) -> Vec<MessageHandler> {
                let cc: Arc<AtomicBool> = Arc::clone(&self.ctx_check);
                let ac: Arc<AtomicBool> = Arc::clone(&self.adapter_check);
                vec![MessageHandler {
                    func: Arc::new(move |_, adapter, handler_ctx| {
                        let cc: Arc<AtomicBool> = Arc::clone(&cc);
                        let ac: Arc<AtomicBool> = Arc::clone(&ac);
                        Box::pin(async move {
                            if handler_ctx.get::<CtxMarker>().is_some() {
                                cc.store(true, Ordering::SeqCst);
                            }
                            if adapter.send("test", &MessageChain::from(""), None).await.is_ok() {
                                ac.store(true, Ordering::SeqCst);
                            }
                        })
                    }),
                    rule: None,
                }]
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(DepsPlugin { ctx_check: cu, adapter_check: au });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let mut event = make_event("check");
        event.set_callback(|_| Box::pin(async {}));

        let _ = actor_ref.tell(HandleEvent {
            event,
            adapter: Arc::new(MockAdapter),
            ctx,
        }).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(ctx_used.load(Ordering::SeqCst), "ctx should be passed and accessible");
        assert!(adapter_used.load(Ordering::SeqCst), "adapter should be passed and callable");
        Ok(())
    }
}
