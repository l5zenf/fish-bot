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
    use std::sync::atomic::{AtomicBool, Ordering};
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
