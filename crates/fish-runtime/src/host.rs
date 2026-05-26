use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use kameo::actor::{ActorRef, Spawn};

use fish_core::AdapterEventSink;
use fish_core::event::{MessageEvent, SystemEvent};

use crate::actor::{HandleEvent, HandleSystemEvent, PluginActor};
use crate::handlers::RouteHint;
use crate::{ActorBusHandle, BaseAdapter, Ctx, Plugin, Result, Telemetry};

pub struct RuntimeHost {
    adapter: Arc<dyn BaseAdapter>,
    router: Arc<RuntimeRouter>,
}

impl RuntimeHost {
    pub fn new(
        adapter: Arc<dyn BaseAdapter>,
        plugins: Vec<Arc<dyn Plugin>>,
        ctx: Arc<Ctx>,
        telemetry: Arc<Telemetry>,
    ) -> Self {
        adapter.register_context(ctx.as_ref());

        if ctx.get::<ActorBusHandle>().is_none() {
            ctx.insert(ActorBusHandle::runtime_default());
        }

        let plugin_refs = plugins
            .into_iter()
            .map(|plugin| {
                let plugin_ref = PluginActor::spawn(PluginActor::new(Arc::clone(&plugin)));
                (plugin_ref, plugin)
            })
            .collect();

        let router = Arc::new(RuntimeRouter::new(
            Arc::clone(&adapter),
            plugin_refs,
            ctx,
            telemetry,
        ));

        Self { adapter, router }
    }

    pub fn with_plugins(adapter: Arc<dyn BaseAdapter>, plugins: Vec<Arc<dyn Plugin>>) -> Self {
        Self::new(
            adapter,
            plugins,
            Arc::new(Ctx::new()),
            Arc::new(Telemetry::new()),
        )
    }

    pub async fn run(&self) -> Result<()> {
        let sink: Arc<dyn AdapterEventSink> = Arc::new(RuntimeEventSink {
            router: Arc::clone(&self.router),
        });
        self.adapter.run(sink).await
    }
}

#[derive(Clone)]
struct RouteTarget {
    plugin_ref: ActorRef<PluginActor>,
    handler_id: String,
}

struct RuntimeRouter {
    adapter: Arc<dyn BaseAdapter>,
    exact_routes: HashMap<String, Vec<RouteTarget>>,
    prefix_routes: Vec<(String, RouteTarget)>,
    keyword_routes: Vec<(String, RouteTarget)>,
    fallback_routes: Vec<RouteTarget>,
    event_routes: HashMap<String, Vec<RouteTarget>>,
    ctx: Arc<Ctx>,
    telemetry: Arc<Telemetry>,
}

impl RuntimeRouter {
    fn new(
        adapter: Arc<dyn BaseAdapter>,
        plugin_refs: Vec<(ActorRef<PluginActor>, Arc<dyn Plugin>)>,
        ctx: Arc<Ctx>,
        telemetry: Arc<Telemetry>,
    ) -> Self {
        let mut exact_routes: HashMap<String, Vec<RouteTarget>> = HashMap::new();
        let mut prefix_routes = Vec::new();
        let mut keyword_routes = Vec::new();
        let mut fallback_routes = Vec::new();
        let mut event_routes: HashMap<String, Vec<RouteTarget>> = HashMap::new();

        for (plugin_ref, plugin) in &plugin_refs {
            for handler in plugin.message_handlers() {
                let target = RouteTarget {
                    plugin_ref: plugin_ref.clone(),
                    handler_id: handler.id.clone(),
                };
                match &handler.route {
                    RouteHint::Exact(patterns) => {
                        for pattern in patterns {
                            exact_routes
                                .entry(pattern.clone())
                                .or_default()
                                .push(RouteTarget {
                                    plugin_ref: plugin_ref.clone(),
                                    handler_id: handler.id.clone(),
                                });
                        }
                    }
                    RouteHint::Prefix(patterns) => {
                        for pattern in patterns {
                            prefix_routes.push((
                                pattern.clone(),
                                RouteTarget {
                                    plugin_ref: plugin_ref.clone(),
                                    handler_id: handler.id.clone(),
                                },
                            ));
                        }
                    }
                    RouteHint::Keyword(patterns) => {
                        for pattern in patterns {
                            keyword_routes.push((
                                pattern.clone(),
                                RouteTarget {
                                    plugin_ref: plugin_ref.clone(),
                                    handler_id: handler.id.clone(),
                                },
                            ));
                        }
                    }
                    RouteHint::Regex | RouteHint::Fallback => fallback_routes.push(target),
                }
            }

            for handler in plugin.event_handlers() {
                event_routes
                    .entry(handler.event_type.clone())
                    .or_default()
                    .push(RouteTarget {
                        plugin_ref: plugin_ref.clone(),
                        handler_id: handler.id.clone(),
                    });
            }
        }

        Self {
            adapter,
            exact_routes,
            prefix_routes,
            keyword_routes,
            fallback_routes,
            event_routes,
            ctx,
            telemetry,
        }
    }

    async fn dispatch_message(&self, event: MessageEvent) {
        self.telemetry
            .messages_received
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let text = event.plain_text();
        let trimmed = text.trim().to_string();
        let mut targets: Vec<(ActorRef<PluginActor>, String)> = Vec::new();

        if let Some(hits) = self.exact_routes.get(&trimmed) {
            self.telemetry
                .exact_route_hits
                .fetch_add(hits.len(), std::sync::atomic::Ordering::Relaxed);
            for target in hits {
                targets.push((target.plugin_ref.clone(), target.handler_id.clone()));
            }
        }

        for (prefix, target) in &self.prefix_routes {
            if text.starts_with(prefix) {
                self.telemetry
                    .prefix_route_hits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                targets.push((target.plugin_ref.clone(), target.handler_id.clone()));
            }
        }

        for (keyword, target) in &self.keyword_routes {
            if text.contains(keyword) {
                self.telemetry
                    .keyword_route_hits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                targets.push((target.plugin_ref.clone(), target.handler_id.clone()));
            }
        }

        let fallback_count = self.fallback_routes.len();
        if fallback_count > 0 {
            self.telemetry
                .fallback_dispatches
                .fetch_add(fallback_count, std::sync::atomic::Ordering::Relaxed);
        }
        for target in &self.fallback_routes {
            targets.push((target.plugin_ref.clone(), target.handler_id.clone()));
        }

        if targets.is_empty() {
            self.telemetry
                .unmatched_messages
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        for (plugin_ref, handler_id) in targets {
            self.telemetry
                .handler_dispatches
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let _ = plugin_ref
                .tell(HandleEvent {
                    event: event.clone(),
                    adapter: Arc::clone(&self.adapter),
                    ctx: Arc::clone(&self.ctx),
                    handler_id: Some(handler_id),
                    telemetry: Arc::clone(&self.telemetry),
                })
                .await;
        }
    }

    async fn dispatch_system(&self, event: Arc<SystemEvent>) {
        let targets = self
            .event_routes
            .get(&event.event_type)
            .cloned()
            .unwrap_or_default();
        if targets.is_empty() {
            tracing::debug!(event_type = %event.event_type, "no handlers for system event");
            return;
        }

        for target in targets {
            let _ = target
                .plugin_ref
                .tell(HandleSystemEvent {
                    event: Arc::clone(&event),
                    adapter: Arc::clone(&self.adapter),
                    ctx: Arc::clone(&self.ctx),
                    handler_id: Some(target.handler_id),
                    telemetry: Arc::clone(&self.telemetry),
                })
                .await;
        }
    }
}

struct RuntimeEventSink {
    router: Arc<RuntimeRouter>,
}

#[async_trait]
impl AdapterEventSink for RuntimeEventSink {
    async fn handle_message(&self, event: MessageEvent) -> Result<()> {
        self.router.dispatch_message(event).await;
        Ok(())
    }

    async fn handle_system(&self, event: SystemEvent) -> Result<()> {
        self.router.dispatch_system(Arc::new(event)).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::handlers::{HandlerContext, MessageHandler, RouteHint};
    use crate::plugin::PluginMetadata;
    use async_trait::async_trait;
    use fish_core::AdapterEventSink;
    use fish_core::error::{AppError, Result};
    use fish_core::message::MessageChain;
    use fish_core::rule::is_fullmatch;

    #[derive(Clone)]
    struct AdapterMarker(&'static str);

    struct MockAdapter;
    #[async_trait]
    impl BaseAdapter for MockAdapter {
        async fn send(&self, _: &str, _: &MessageChain, _: Option<&str>) -> Result<()> {
            Ok(())
        }
        async fn run(&self, _: Arc<dyn AdapterEventSink>) -> Result<()> {
            Ok(())
        }
    }

    struct CounterPlugin {
        meta: PluginMetadata,
        handlers: Vec<MessageHandler>,
    }
    impl Plugin for CounterPlugin {
        fn metadata(&self) -> &PluginMetadata {
            &self.meta
        }
        fn message_handlers(&self) -> &[MessageHandler] {
            &self.handlers
        }
    }

    fn make_counter_plugin(count: Arc<AtomicUsize>) -> CounterPlugin {
        CounterPlugin {
            meta: PluginMetadata {
                id: "counter".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "counter",
                RouteHint::Exact(vec!["/ping".into()]),
                Some(is_fullmatch(["/ping"])),
                Arc::new(move |_: HandlerContext| {
                    let c = Arc::clone(&count);
                    Box::pin(async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }),
            )],
        }
    }

    fn make_event(text: &str) -> MessageEvent {
        MessageEvent::new(
            "cid".into(),
            "sender".into(),
            "name".into(),
            MessageChain::from(text),
            Default::default(),
        )
    }

    #[test]
    fn t4_2_router_builds_empty_tables() {
        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());
        let router = RuntimeRouter::new(adapter, vec![], ctx, Arc::new(Telemetry::new()));
        assert!(router.exact_routes.is_empty());
        assert!(router.fallback_routes.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_3_dispatch_replies_via_adapter() {
        let recorded = Arc::new(tokio::sync::Mutex::new(String::new()));
        let recorded_clone = Arc::clone(&recorded);

        struct RecordAdapter(Arc<tokio::sync::Mutex<String>>);
        #[async_trait]
        impl BaseAdapter for RecordAdapter {
            async fn send(
                &self,
                _target: &str,
                msg: &MessageChain,
                _cid: Option<&str>,
            ) -> Result<()> {
                let mut guard = self.0.lock().await;
                *guard = msg.summary();
                Ok(())
            }
            async fn run(&self, _: Arc<dyn AdapterEventSink>) -> Result<()> {
                Ok(())
            }
        }

        let adapter: Arc<dyn BaseAdapter> = Arc::new(RecordAdapter(recorded_clone));
        let ctx = Arc::new(Ctx::new());

        struct EchoPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for EchoPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(EchoPlugin {
            meta: PluginMetadata {
                id: "echo".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "echo",
                RouteHint::Fallback,
                None,
                Arc::new(|cx: HandlerContext| {
                    let content = cx.event.plain_text();
                    Box::pin(async move {
                        let _ = cx.reply(content).await;
                        Ok(())
                    })
                }),
            )],
        });
        let plugin_for_router = Arc::clone(&plugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let router = RuntimeRouter::new(
            adapter,
            vec![(plugin_ref, plugin_for_router)],
            ctx,
            Arc::new(Telemetry::new()),
        );

        router.dispatch_message(make_event("/ping")).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let guard = recorded.lock().await;
        assert_eq!(*guard, "/ping");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_4_dispatch_fan_out() {
        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());

        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));

        let plugin1: Arc<dyn Plugin> = Arc::new(make_counter_plugin(Arc::clone(&count1)));
        let plugin2: Arc<dyn Plugin> = Arc::new(make_counter_plugin(Arc::clone(&count2)));
        let p1 = Arc::clone(&plugin1);
        let p2 = Arc::clone(&plugin2);
        let pref1 = PluginActor::spawn(PluginActor::new(plugin1));
        let pref2 = PluginActor::spawn(PluginActor::new(plugin2));

        let router = RuntimeRouter::new(
            adapter,
            vec![(pref1, p1), (pref2, p2)],
            ctx,
            Arc::new(Telemetry::new()),
        );

        router.dispatch_message(make_event("/ping")).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        assert_eq!(count1.load(Ordering::SeqCst), 1);
        assert_eq!(count2.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_5_dispatch_empty_plugin_refs() {
        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());
        let router = RuntimeRouter::new(adapter, vec![], ctx, Arc::new(Telemetry::new()));
        router.dispatch_message(make_event("/ping")).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_7_dispatch_reply_send_error() -> anyhow::Result<()> {
        struct ErrorAdapter;
        #[async_trait]
        impl BaseAdapter for ErrorAdapter {
            async fn send(&self, _: &str, _: &MessageChain, _: Option<&str>) -> Result<()> {
                Err(AppError::http("simulated error"))
            }
            async fn run(&self, _: Arc<dyn AdapterEventSink>) -> Result<()> {
                Ok(())
            }
        }

        let adapter: Arc<dyn BaseAdapter> = Arc::new(ErrorAdapter);
        let ctx = Arc::new(Ctx::new());

        struct ReplyPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for ReplyPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(ReplyPlugin {
            meta: PluginMetadata {
                id: "reply".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "reply",
                RouteHint::Fallback,
                None,
                Arc::new(|cx: HandlerContext| {
                    Box::pin(async move {
                        let _ = cx.reply("reply").await;
                        Ok(())
                    })
                }),
            )],
        });
        let plugin_for_router = Arc::clone(&plugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let router = RuntimeRouter::new(
            adapter,
            vec![(plugin_ref, plugin_for_router)],
            ctx,
            Arc::new(Telemetry::new()),
        );

        router.dispatch_message(make_event("/test")).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_8_dispatch_rule_not_matching() -> anyhow::Result<()> {
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);
        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());

        struct SelectivePlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for SelectivePlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(SelectivePlugin {
            meta: PluginMetadata {
                id: "selective".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "selective",
                RouteHint::Fallback,
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
        let plugin_for_router = Arc::clone(&plugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let router = RuntimeRouter::new(
            adapter,
            vec![(plugin_ref, plugin_for_router)],
            ctx,
            Arc::new(Telemetry::new()),
        );

        router.dispatch_message(make_event("/skip")).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        assert!(!called.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_9_dispatch_reply_multi_segment() -> anyhow::Result<()> {
        let recorded = Arc::new(tokio::sync::Mutex::new(String::new()));
        let recorded_clone = Arc::clone(&recorded);

        struct RecordAdapter(Arc<tokio::sync::Mutex<String>>);
        #[async_trait]
        impl BaseAdapter for RecordAdapter {
            async fn send(
                &self,
                _target: &str,
                msg: &MessageChain,
                _cid: Option<&str>,
            ) -> Result<()> {
                let mut guard = self.0.lock().await;
                *guard = msg.summary();
                Ok(())
            }
            async fn run(&self, _: Arc<dyn AdapterEventSink>) -> Result<()> {
                Ok(())
            }
        }

        let adapter: Arc<dyn BaseAdapter> = Arc::new(RecordAdapter(recorded_clone));
        let ctx = Arc::new(Ctx::new());

        struct MultiReplyPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for MultiReplyPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn message_handlers(&self) -> &[MessageHandler] {
                &self.handlers
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(MultiReplyPlugin {
            meta: PluginMetadata {
                id: "multi_reply".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "multi_reply",
                RouteHint::Fallback,
                None,
                Arc::new(|cx: HandlerContext| {
                    Box::pin(async move {
                        let _ = cx.reply("multi segment reply").await;
                        Ok(())
                    })
                }),
            )],
        });
        let plugin_for_router = Arc::clone(&plugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let router = RuntimeRouter::new(
            adapter,
            vec![(plugin_ref, plugin_for_router)],
            ctx,
            Arc::new(Telemetry::new()),
        );

        router.dispatch_message(make_event("/test")).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let guard = recorded.lock().await;
        assert_eq!(*guard, "multi segment reply");
        Ok(())
    }

    #[test]
    fn t4_10_runtime_host_registers_adapter_context() {
        struct ContextAdapter;

        #[async_trait]
        impl BaseAdapter for ContextAdapter {
            fn register_context(&self, ctx: &Ctx) {
                ctx.insert(AdapterMarker("fish-client"));
            }

            async fn send(&self, _: &str, _: &MessageChain, _: Option<&str>) -> Result<()> {
                Ok(())
            }

            async fn run(&self, _: Arc<dyn AdapterEventSink>) -> Result<()> {
                Ok(())
            }
        }

        let ctx = Arc::new(Ctx::new());
        let _host = RuntimeHost::new(
            Arc::new(ContextAdapter),
            vec![],
            Arc::clone(&ctx),
            Arc::new(Telemetry::new()),
        );

        let marker = ctx
            .get::<AdapterMarker>()
            .expect("adapter should register app context");
        assert_eq!(marker.0, "fish-client");
    }
}
