use std::sync::Arc;
use std::time::Duration;

use kameo::message::{Context, Message};
use kameo::prelude::*;

use crate::handlers::{
    EventHandlerContext, EventHandlerFunc, HandlerContext, MessageHandler, RouteHint,
};
use crate::runtime::{QueueStrategy, RuntimeConfig};
use crate::{BaseAdapter, Plugin};
use fish_core::ctx::Ctx;
use fish_core::event::{MessageEvent, SystemEvent};
use fish_core::telemetry::Telemetry;

mod task_runner;

use self::task_runner::{TaskFuture, TaskScheduler};

/// Plugin actor — wraps a Plugin and processes HandleEvent messages in isolation.
/// Each plugin runs in its own kameo actor task with automatic panic recovery.
/// Concurrency and queueing are delegated to an internal scheduler.
#[derive(Actor)]
pub struct PluginActor {
    plugin: Arc<dyn Plugin>,
    /// Handler id → index into plugin.message_handlers() for O(1) lookup.
    handler_index: std::collections::HashMap<String, usize>,
    scheduler: TaskScheduler,
    /// Optional mutable plugin state (for stateful plugins).
    pub(crate) plugin_state: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

impl PluginActor {
    /// Create a new PluginActor, reading `RuntimeConfig` from the plugin itself.
    pub fn new(plugin: Arc<dyn Plugin>) -> Self {
        let config = plugin.runtime_config();
        Self::with_runtime(plugin, config)
    }

    /// Create a PluginActor with an explicit queue strategy (other config from plugin default).
    #[allow(dead_code)]
    pub fn with_strategy(plugin: Arc<dyn Plugin>, strategy: QueueStrategy) -> Self {
        let mut config = plugin.runtime_config();
        config.queue_strategy = strategy;
        Self::with_runtime(plugin, config)
    }

    /// Create a PluginActor with a full explicit runtime configuration.
    pub fn with_runtime(plugin: Arc<dyn Plugin>, config: RuntimeConfig) -> Self {
        let handler_index = plugin
            .message_handlers()
            .iter()
            .enumerate()
            .map(|(i, h)| (h.id.clone(), i))
            .collect();

        let plugin_state = plugin.initial_state();

        Self {
            plugin,
            handler_index,
            scheduler: TaskScheduler::new(&config),
            plugin_state,
        }
    }

    /// Accessor for the wrapped plugin definition.
    pub fn plugin(&self) -> &Arc<dyn Plugin> {
        &self.plugin
    }

    /// Accessor for the precomputed handler lookup table.
    pub fn handler_index(&self) -> &std::collections::HashMap<String, usize> {
        &self.handler_index
    }

    async fn dispatch_task_or_enqueue(
        &self,
        handler_id: &str,
        handler_timeout: Duration,
        plugin_id: &str,
        future: TaskFuture,
        telemetry: Arc<Telemetry>,
    ) {
        self.scheduler
            .dispatch_task_or_enqueue(handler_id, handler_timeout, plugin_id, future, telemetry)
            .await;
    }

    /// Try to acquire a permit or enqueue the task per the queue strategy.
    pub(crate) async fn dispatch_or_enqueue(
        &self,
        handler: &MessageHandler,
        plugin_id: &str,
        event: MessageEvent,
        adapter: Arc<dyn BaseAdapter>,
        ctx: Arc<Ctx>,
        telemetry: Arc<Telemetry>,
    ) {
        let future = (handler.func.clone())(HandlerContext::__new(
            event,
            adapter,
            ctx,
            Arc::clone(&telemetry),
            self.plugin_state.clone(),
        ));
        self.dispatch_task_or_enqueue(&handler.id, handler.timeout, plugin_id, future, telemetry)
            .await;
    }

    pub(crate) async fn dispatch_event_or_enqueue(
        &self,
        handler_id: &str,
        plugin_id: &str,
        future: TaskFuture,
        telemetry: Arc<Telemetry>,
    ) {
        self.dispatch_task_or_enqueue(
            handler_id,
            self.scheduler.default_timeout(),
            plugin_id,
            future,
            telemetry,
        )
        .await;
    }
}

pub struct HandleEvent {
    pub event: MessageEvent,
    pub adapter: Arc<dyn BaseAdapter>,
    pub ctx: Arc<Ctx>,
    pub handler_id: Option<String>,
    pub telemetry: Arc<Telemetry>,
}

impl Message<HandleEvent> for PluginActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: HandleEvent,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let plugin_id = self.plugin().metadata().id.clone();
        let handlers = self.plugin().message_handlers();

        let matched_handlers: Vec<&MessageHandler> = match &msg.handler_id {
            Some(hid) => match self
                .handler_index()
                .get(hid)
                .and_then(|&idx| handlers.get(idx))
            {
                Some(handler) => {
                    let matched = match handler.route {
                        RouteHint::Exact(_) | RouteHint::Prefix(_) | RouteHint::Keyword(_) => true,
                        RouteHint::Regex | RouteHint::Fallback => match &handler.rule {
                            Some(rule) => rule.check(&msg.event),
                            None => true,
                        },
                    };
                    if matched { vec![handler] } else { vec![] }
                }
                None => vec![],
            },
            None => handlers
                .iter()
                .filter(|h| match &h.rule {
                    Some(rule) => rule.check(&msg.event),
                    None => true,
                })
                .collect(),
        };

        for handler in matched_handlers {
            self.dispatch_or_enqueue(
                handler,
                &plugin_id,
                msg.event.clone(),
                Arc::clone(&msg.adapter),
                Arc::clone(&msg.ctx),
                Arc::clone(&msg.telemetry),
            )
            .await;
        }
    }
}

pub struct HandleSystemEvent {
    pub event: Arc<SystemEvent>,
    pub adapter: Arc<dyn BaseAdapter>,
    pub ctx: Arc<Ctx>,
    pub handler_id: Option<String>,
    pub telemetry: Arc<Telemetry>,
}

impl Message<HandleSystemEvent> for PluginActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: HandleSystemEvent,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let plugin_id = self.plugin().metadata().id.clone();
        let handlers = self.plugin().event_handlers();

        let funcs: Vec<(String, EventHandlerFunc)> = match &msg.handler_id {
            Some(hid) => handlers
                .iter()
                .filter(|h| &h.id == hid)
                .map(|h| (h.id.clone(), Arc::clone(&h.func)))
                .collect(),
            None => handlers
                .iter()
                .filter(|h| h.event_type == msg.event.event_type)
                .map(|h| (h.id.clone(), Arc::clone(&h.func)))
                .collect(),
        };

        for (handler_id, func) in funcs {
            let future = (func)(EventHandlerContext::__new(
                Arc::clone(&msg.event),
                Arc::clone(&msg.adapter),
                Arc::clone(&msg.ctx),
                Arc::clone(&msg.telemetry),
                self.plugin_state.clone(),
            ));
            self.dispatch_event_or_enqueue(
                &handler_id,
                &plugin_id,
                future,
                Arc::clone(&msg.telemetry),
            )
            .await;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::handlers::{HandlerContext, MessageHandler, RouteHint};
    use crate::plugin::PluginMetadata;
    use crate::{BaseAdapter, Plugin};
    use async_trait::async_trait;
    use fish_core::AdapterEventSink;
    use fish_core::ctx::Ctx;
    use fish_core::event::MessageEvent;
    use fish_core::message::{MessageChain, MessageSegment};
    use fish_core::rule::is_fullmatch;
    use fish_core::telemetry::Telemetry;
    use kameo::actor::Spawn;

    struct MockAdapter;
    #[async_trait]
    impl BaseAdapter for MockAdapter {
        async fn send(
            &self,
            _: &str,
            _: &MessageChain,
            _: Option<&str>,
        ) -> fish_core::error::Result<()> {
            Ok(())
        }
        async fn run(&self, _: Arc<dyn AdapterEventSink>) -> fish_core::error::Result<()> {
            Ok(())
        }
    }

    struct TestPlugin {
        meta: PluginMetadata,
        handlers: Vec<MessageHandler>,
    }
    impl Plugin for TestPlugin {
        fn metadata(&self) -> &PluginMetadata {
            &self.meta
        }
        fn message_handlers(&self) -> &[MessageHandler] {
            &self.handlers
        }
    }

    fn make_test_plugin() -> TestPlugin {
        TestPlugin {
            meta: PluginMetadata {
                id: "test".into(),
                name: "test".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "ping",
                RouteHint::Exact(vec!["/ping".into()]),
                Some(is_fullmatch(["/ping"])),
                Arc::new(|cx: HandlerContext| {
                    let reply = cx.event.plain_text();
                    Box::pin(async move {
                        let _ = cx.reply(MessageSegment::text(reply)).await;
                        Ok(())
                    })
                }),
            )],
        }
    }

    fn make_event(text: &str) -> MessageEvent {
        MessageEvent::new(
            "cid".into(),
            "uid".into(),
            "name".into(),
            MessageChain::from(text),
            serde_json::json!({}),
        )
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn t2_5_actor_new() {
        let plugin: Arc<dyn Plugin> = Arc::new(make_test_plugin());
        let actor = PluginActor::new(plugin);
        let _ref = PluginActor::spawn(actor);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_5b_with_strategy_overrides_runtime_config() {
        let plugin: Arc<dyn Plugin> = Arc::new(make_test_plugin());
        let actor = PluginActor::with_strategy(plugin, QueueStrategy::DropOldest(8));
        assert_eq!(actor.plugin().metadata().id, "test");
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
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(FlagPlugin {
            meta: PluginMetadata {
                id: "flag".into(),
                name: "".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "flag",
                RouteHint::Exact(vec!["/ping".into()]),
                Some(is_fullmatch(["/ping"])),
                Arc::new(move |_: HandlerContext| {
                    let f = Arc::clone(&called_clone);
                    Box::pin(async move {
                        f.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                }),
            )],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let _ = actor_ref
            .tell(HandleEvent {
                event: make_event("/ping"),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: None,
                telemetry: Arc::new(Telemetry::new()),
            })
            .await;

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
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(FlagPlugin {
            meta: PluginMetadata {
                id: "flag".into(),
                name: "".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "flag",
                RouteHint::Exact(vec!["/ping".into()]),
                Some(is_fullmatch(["/ping"])),
                Arc::new(move |_: HandlerContext| {
                    let f = Arc::clone(&called_clone);
                    Box::pin(async move {
                        f.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                }),
            )],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let _ = actor_ref
            .tell(HandleEvent {
                event: make_event("/pong"),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: None,
                telemetry: Arc::new(Telemetry::new()),
            })
            .await;

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
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(NoRulePlugin {
            meta: PluginMetadata {
                id: "norule".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "h1",
                RouteHint::Fallback,
                None,
                Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
            )],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let _ = actor_ref
            .tell(HandleEvent {
                event: make_event("anything"),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: None,
                telemetry: Arc::new(Telemetry::new()),
            })
            .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_9_handler_panic_does_not_propagate() {
        struct PanicPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for PanicPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(PanicPlugin {
            meta: PluginMetadata {
                id: "panic".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "panic",
                RouteHint::Fallback,
                None,
                Arc::new(|_: HandlerContext| {
                    Box::pin(async { std::panic::panic_any("intentional panic") })
                }),
            )],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let _ = actor_ref
            .tell(HandleEvent {
                event: make_event("anything"),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: None,
                telemetry: Arc::new(Telemetry::new()),
            })
            .await;
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
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let count = Arc::clone(&call_count);
        let plugin: Arc<dyn Plugin> = Arc::new(MultiHandlerPlugin {
            meta: PluginMetadata {
                id: "multi".into(),
                ..Default::default()
            },
            handlers: vec![
                MessageHandler::new(
                    "h1",
                    RouteHint::Fallback,
                    None,
                    Arc::new({
                        let c = Arc::clone(&count);
                        move |_: HandlerContext| {
                            let c2 = Arc::clone(&c);
                            Box::pin(async move {
                                c2.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            })
                        }
                    }),
                ),
                MessageHandler::new(
                    "h2",
                    RouteHint::Fallback,
                    None,
                    Arc::new({
                        let c = Arc::clone(&count);
                        move |_: HandlerContext| {
                            let c2 = Arc::clone(&c);
                            Box::pin(async move {
                                c2.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            })
                        }
                    }),
                ),
                MessageHandler::new(
                    "h3",
                    RouteHint::Fallback,
                    None,
                    Arc::new({
                        let c = Arc::clone(&count);
                        move |_: HandlerContext| {
                            let c2 = Arc::clone(&c);
                            Box::pin(async move {
                                c2.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            })
                        }
                    }),
                ),
            ],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let _ = actor_ref
            .tell(HandleEvent {
                event: make_event("test"),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: None,
                telemetry: Arc::new(Telemetry::new()),
            })
            .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            3,
            "all 3 handlers should execute"
        );
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
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(SkipPlugin {
            meta: PluginMetadata {
                id: "skip".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "skip",
                RouteHint::Exact(vec!["/run".into()]),
                Some(is_fullmatch(["/run"])),
                Arc::new(move |_: HandlerContext| {
                    let f = Arc::clone(&called_clone);
                    Box::pin(async move {
                        f.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                }),
            )],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let _ = actor_ref
            .tell(HandleEvent {
                event: make_event("/skip"),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: None,
                telemetry: Arc::new(Telemetry::new()),
            })
            .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(
            !called.load(Ordering::SeqCst),
            "handler should not be called when rule doesn't match"
        );
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
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let cu = Arc::clone(&ctx_used);
        let au = Arc::clone(&adapter_used);
        let plugin: Arc<dyn Plugin> = Arc::new(DepsPlugin {
            meta: PluginMetadata {
                id: "deps".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "deps",
                RouteHint::Fallback,
                None,
                Arc::new(move |cx: HandlerContext| {
                    let cc = Arc::clone(&cu);
                    let ac = Arc::clone(&au);
                    let adapter = cx.adapter;
                    let app_ctx = cx.app_ctx;
                    Box::pin(async move {
                        if app_ctx.get::<CtxMarker>().is_some() {
                            cc.store(true, Ordering::SeqCst);
                        }
                        let _ = adapter.send("test", &MessageChain::from(""), None).await;
                        ac.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                }),
            )],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let _ = actor_ref
            .tell(HandleEvent {
                event: make_event("check"),
                adapter: Arc::new(MockAdapter),
                ctx,
                handler_id: None,
                telemetry: Arc::new(Telemetry::new()),
            })
            .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(
            ctx_used.load(Ordering::SeqCst),
            "ctx should be passed and accessible"
        );
        assert!(
            adapter_used.load(Ordering::SeqCst),
            "adapter should be passed and callable"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_30_zero_handlers_does_not_panic() -> anyhow::Result<()> {
        struct EmptyPlugin {
            meta: PluginMetadata,
        }
        impl Plugin for EmptyPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(EmptyPlugin {
            meta: PluginMetadata {
                id: "empty".into(),
                ..Default::default()
            },
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let _ = actor_ref
            .tell(HandleEvent {
                event: make_event("anything"),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: None,
                telemetry: Arc::new(Telemetry::new()),
            })
            .await;
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
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let c = Arc::clone(&call_count);
        let plugin: Arc<dyn Plugin> = Arc::new(MixedPlugin {
            meta: PluginMetadata {
                id: "mixed".into(),
                ..Default::default()
            },
            handlers: vec![
                MessageHandler::new(
                    "ping_rule",
                    RouteHint::Exact(vec!["/ping".into()]),
                    Some(is_fullmatch(["/ping"])),
                    Arc::new({
                        let count = Arc::clone(&c);
                        move |_: HandlerContext| {
                            let c2 = Arc::clone(&count);
                            Box::pin(async move {
                                c2.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            })
                        }
                    }),
                ),
                MessageHandler::new(
                    "pong_rule",
                    RouteHint::Exact(vec!["/pong".into()]),
                    Some(is_fullmatch(["/pong"])),
                    Arc::new({
                        let count = Arc::clone(&c);
                        move |_: HandlerContext| {
                            let c2 = Arc::clone(&count);
                            Box::pin(async move {
                                c2.fetch_add(10, Ordering::SeqCst);
                                Ok(())
                            })
                        }
                    }),
                ),
                MessageHandler::new(
                    "catchall",
                    RouteHint::Fallback,
                    None,
                    Arc::new({
                        let count = Arc::clone(&c);
                        move |_: HandlerContext| {
                            let c2 = Arc::clone(&count);
                            Box::pin(async move {
                                c2.fetch_add(100, Ordering::SeqCst);
                                Ok(())
                            })
                        }
                    }),
                ),
            ],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let _ = actor_ref
            .tell(HandleEvent {
                event: make_event("/ping"),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: None,
                telemetry: Arc::new(Telemetry::new()),
            })
            .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            101,
            "only matching rule and no-rule handlers should execute"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_32_handler_with_plugin_name() -> anyhow::Result<()> {
        struct NamedPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for NamedPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(NamedPlugin {
            meta: PluginMetadata {
                id: "named".into(),
                name: "TestName".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "h1",
                RouteHint::Fallback,
                None,
                Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
            )],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let _ = actor_ref
            .tell(HandleEvent {
                event: make_event("test"),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: None,
                telemetry: Arc::new(Telemetry::new()),
            })
            .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_33_multiple_events_to_same_actor() -> anyhow::Result<()> {
        let counter: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

        struct CountPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for CountPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let c = Arc::clone(&counter);
        let plugin: Arc<dyn Plugin> = Arc::new(CountPlugin {
            meta: PluginMetadata {
                id: "count".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "count",
                RouteHint::Fallback,
                None,
                Arc::new(move |_: HandlerContext| {
                    let c2 = Arc::clone(&c);
                    Box::pin(async move {
                        c2.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }),
            )],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        for _ in 0..3 {
            let _ = actor_ref
                .tell(HandleEvent {
                    event: make_event("test"),
                    adapter: Arc::new(MockAdapter),
                    ctx: Arc::new(Ctx::new()),
                    handler_id: None,
                    telemetry: Arc::new(Telemetry::new()),
                })
                .await;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            3,
            "all 3 events should be handled"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t2_34_plugin_with_custom_metadata_name() -> anyhow::Result<()> {
        struct CustomMetaPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for CustomMetaPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(CustomMetaPlugin {
            meta: PluginMetadata {
                id: "custom_meta".into(),
                name: "元数据测试".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "h1",
                RouteHint::Fallback,
                None,
                Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
            )],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));

        let _ = actor_ref
            .tell(HandleEvent {
                event: make_event("test"),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: None,
                telemetry: Arc::new(Telemetry::new()),
            })
            .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        Ok(())
    }
}
