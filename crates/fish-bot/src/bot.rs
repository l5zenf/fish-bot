use std::collections::HashMap;
use std::sync::Arc;

use kameo::prelude::*;
use kameo::message::{Context, Message};

use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_core::event::MessageEvent;
use fish_core::message::{MessageChain, MessageSegment};
use fish_core::telemetry::Telemetry;
use fish_plugin::plugin::actor::{HandleEvent, PluginActor};
use fish_plugin::plugin::RouteHint;

/// A routing target resolved at startup — maps a route to a specific handler.
struct RouteTarget {
    plugin_ref: ActorRef<PluginActor>,
    handler_id: String,
}

/// Bot actor — receives MessageEvents and dispatches to PluginActors
/// via a pre-compiled routing table instead of scanning all plugins.
#[derive(Actor)]
pub struct Bot {
    adapter: Arc<dyn BaseAdapter>,
    /// Exact trimmed-text match — O(1) HashMap lookup.
    exact_routes: HashMap<String, Vec<RouteTarget>>,
    /// Handlers whose prefix was matched at routing time.
    prefix_routes: Vec<(String, RouteTarget)>,
    /// Handlers whose keyword was matched at routing time.
    keyword_routes: Vec<(String, RouteTarget)>,
    /// Handlers Bot cannot pre-filter (Regex / Fallback) — always dispatched.
    /// PluginActor still checks the rule for these.
    fallback_routes: Vec<RouteTarget>,
    ctx: Arc<Ctx>,
    telemetry: Arc<Telemetry>,
}

impl Bot {
    pub fn new(
        adapter: Arc<dyn BaseAdapter>,
        plugin_refs: Vec<(ActorRef<PluginActor>, Arc<dyn fish_plugin::plugin::Plugin>)>,
        ctx: Arc<Ctx>,
        telemetry: Arc<Telemetry>,
    ) -> Self {
        let mut exact_routes: HashMap<String, Vec<RouteTarget>> = HashMap::new();
        let mut prefix_routes = Vec::new();
        let mut keyword_routes = Vec::new();
        let mut fallback_routes = Vec::new();

        // Build routing table from all plugins' handlers
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
                            prefix_routes.push((pattern.clone(), RouteTarget {
                                plugin_ref: plugin_ref.clone(),
                                handler_id: handler.id.clone(),
                            }));
                        }
                    }
                    RouteHint::Keyword(patterns) => {
                        for pattern in patterns {
                            keyword_routes.push((pattern.clone(), RouteTarget {
                                plugin_ref: plugin_ref.clone(),
                                handler_id: handler.id.clone(),
                            }));
                        }
                    }
                    RouteHint::Regex | RouteHint::Fallback => {
                        fallback_routes.push(target);
                    }
                }
            }
        }

        Self {
            adapter,
            exact_routes,
            prefix_routes,
            keyword_routes,
            fallback_routes,
            ctx,
            telemetry,
        }
    }
}

// ---- Messages ----

/// Dispatch a message event — sent by the adapter when a new message arrives.
pub struct DispatchEvent {
    pub event: MessageEvent,
}

impl Message<DispatchEvent> for Bot {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: DispatchEvent,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.telemetry.messages_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Create reply callback bound to this event's sender
        let reply_adapter = Arc::clone(&self.adapter);
        let reply_cid = msg.event.cid.clone();
        let reply_target = msg.event.sender_id.clone();
        let reply_telemetry = Arc::clone(&self.telemetry);

        let mut event = msg.event;
        event.set_callback(move |reply_msg: MessageSegment| {
            let adapter = Arc::clone(&reply_adapter);
            let target = reply_target.clone();
            let cid = reply_cid.clone();
            let telemetry = Arc::clone(&reply_telemetry);
            Box::pin(async move {
                let chain = MessageChain::from(reply_msg);
                if let Err(e) = adapter.send(&target, &chain, Some(&cid)).await {
                    tracing::error!("Failed to send reply: {}", e);
                    telemetry.reply_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            })
        });

        let adapter = Arc::clone(&self.adapter);
        let ctx = Arc::clone(&self.ctx);
        let telemetry = Arc::clone(&self.telemetry);

        // Route the event using the pre-compiled routing table.
        let text = event.plain_text();
        let trimmed = text.trim().to_string();

        let mut targets: Vec<(ActorRef<PluginActor>, String)> = Vec::new();

        // 1. Exact match — O(1) HashMap lookup
        if let Some(hits) = self.exact_routes.get(&trimmed) {
            telemetry.exact_route_hits.fetch_add(hits.len(), std::sync::atomic::Ordering::Relaxed);
            for t in hits {
                targets.push((t.plugin_ref.clone(), t.handler_id.clone()));
            }
        }

        // 2. Prefix match
        for (prefix, t) in &self.prefix_routes {
            if text.starts_with(prefix) {
                telemetry.prefix_route_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                targets.push((t.plugin_ref.clone(), t.handler_id.clone()));
            }
        }

        // 3. Keyword match
        for (kw, t) in &self.keyword_routes {
            if text.contains(kw) {
                telemetry.keyword_route_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                targets.push((t.plugin_ref.clone(), t.handler_id.clone()));
            }
        }

        // 4. Fallback/Regex — always dispatch, PluginActor checks rule
        let fallback_count = self.fallback_routes.len();
        if fallback_count > 0 {
            telemetry.fallback_dispatches.fetch_add(fallback_count, std::sync::atomic::Ordering::Relaxed);
        }
        for t in &self.fallback_routes {
            targets.push((t.plugin_ref.clone(), t.handler_id.clone()));
        }

        // Track unmatched messages before dispatch
        if targets.is_empty() {
            telemetry.unmatched_messages.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // Dispatch each target
        for (plugin_ref, handler_id) in targets {
            telemetry.handler_dispatches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let _ = plugin_ref
                .tell(HandleEvent {
                    event: event.clone(),
                    adapter: Arc::clone(&adapter),
                    ctx: Arc::clone(&ctx),
                    handler_id: Some(handler_id),
                    telemetry: Arc::clone(&telemetry),
                })
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use fish_adapter::adapter::BaseAdapter;
    use fish_core::error::{AppError, Result};
    use fish_core::rule::is_fullmatch;
    use fish_core::telemetry::Telemetry;
    use fish_plugin::plugin::{HandlerContext, MessageHandler, Plugin, PluginMetadata};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use kameo::actor::Spawn;

    struct MockAdapter;
    #[async_trait]
    impl BaseAdapter for MockAdapter {
        fn set_callback(&self, _: Box<dyn Fn(MessageEvent) + Send + Sync>) {}
        async fn send(&self, _: &str, _: &MessageChain, _: Option<&str>) -> Result<()> { Ok(()) }
        async fn run(&self) -> Result<()> { Ok(()) }
    }

    struct CounterPlugin {
        meta: PluginMetadata,
        handlers: Vec<MessageHandler>,
    }
    impl Plugin for CounterPlugin {
        fn metadata(&self) -> &PluginMetadata { &self.meta }
        fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
    }

    fn make_counter_plugin(count: Arc<AtomicUsize>) -> CounterPlugin {
        CounterPlugin {
            meta: PluginMetadata { id: "counter".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("counter", RouteHint::Exact(vec!["/ping".into()]), Some(is_fullmatch(["/ping"])), Arc::new(move |_: HandlerContext| {
                let c = Arc::clone(&count);
                Box::pin(async move { c.fetch_add(1, Ordering::SeqCst); Ok(()) })
            }))],
        }
    }

    fn make_event(text: &str) -> MessageEvent {
        MessageEvent::new(
            "cid".into(), "sender".into(), "name".into(),
            MessageChain::from(text),
            Default::default(),
        )
    }

    #[test]
    fn t4_2_bot_new() {
        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());
        let bot = Bot::new(adapter, vec![], ctx, Arc::new(Telemetry::new()));
        assert!(bot.exact_routes.is_empty());
        assert!(bot.fallback_routes.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_3_dispatch_sets_callback() {
        let recorded = Arc::new(tokio::sync::Mutex::new(String::new()));
        let recorded_clone = Arc::clone(&recorded);

        struct RecordAdapter(Arc<tokio::sync::Mutex<String>>);
        #[async_trait]
        impl BaseAdapter for RecordAdapter {
            fn set_callback(&self, _: Box<dyn Fn(MessageEvent) + Send + Sync>) {}
            async fn send(&self, _target: &str, msg: &MessageChain, _cid: Option<&str>) -> Result<()> {
                let mut guard = self.0.lock().await;
                *guard = msg.summary();
                Ok(())
            }
            async fn run(&self) -> Result<()> { Ok(()) }
        }

        let adapter: Arc<dyn BaseAdapter> = Arc::new(RecordAdapter(recorded_clone));
        let ctx = Arc::new(Ctx::new());

        struct EchoPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for EchoPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(EchoPlugin {
            meta: PluginMetadata { id: "echo".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("echo", RouteHint::Fallback, None, Arc::new(|cx: HandlerContext| {
                let event = cx.event;
                let content = event.plain_text();
                Box::pin(async move {
                    let _ = event.reply(MessageSegment::text(content)).await;
                    Ok(())
                })
            }))],
        });
        let plugin_for_bot = Arc::clone(&plugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let bot_ref = Bot::spawn(Bot::new(adapter, vec![(plugin_ref, plugin_for_bot)], ctx, Arc::new(Telemetry::new())));

        let event = make_event("/ping");

        let _ = bot_ref.tell(DispatchEvent { event }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let guard = recorded.lock().await;
        assert_eq!(*guard, "/ping", "adapter.send should have been called with the echoed message");
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

        let bot_ref = Bot::spawn(Bot::new(adapter, vec![(pref1, p1), (pref2, p2)], ctx, Arc::new(Telemetry::new())));

        let mut event = make_event("/ping");
        event.set_callback(|_| Box::pin(async {}));

        let _ = bot_ref.tell(DispatchEvent { event }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        assert_eq!(count1.load(Ordering::SeqCst), 1);
        assert_eq!(count2.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_5_dispatch_empty_plugin_refs() {
        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());
        let bot_ref = Bot::spawn(Bot::new(adapter, vec![], ctx, Arc::new(Telemetry::new())));

        let mut event = make_event("/ping");
        event.set_callback(|_| Box::pin(async {}));

        let _ = bot_ref.tell(DispatchEvent { event }).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_7_dispatch_reply_send_error() -> anyhow::Result<()> {
        struct ErrorAdapter;
        #[async_trait]
        impl BaseAdapter for ErrorAdapter {
            fn set_callback(&self, _: Box<dyn Fn(MessageEvent) + Send + Sync>) {}
            async fn send(&self, _: &str, _: &MessageChain, _: Option<&str>) -> Result<()> {
                Err(AppError::http("simulated error"))
            }
            async fn run(&self) -> Result<()> { Ok(()) }
        }

        let adapter: Arc<dyn BaseAdapter> = Arc::new(ErrorAdapter);
        let ctx = Arc::new(Ctx::new());

        struct ReplyPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for ReplyPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(ReplyPlugin {
            meta: PluginMetadata { id: "reply".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("reply", RouteHint::Fallback, None, Arc::new(|cx: HandlerContext| {
                Box::pin(async move {
                    let _ = cx.event.reply(MessageSegment::text("reply")).await;
                    Ok(())
                })
            }))],
        });
        let plugin_for_bot = Arc::clone(&plugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let bot_ref = Bot::spawn(Bot::new(adapter, vec![(plugin_ref, plugin_for_bot)], ctx, Arc::new(Telemetry::new())));

        let mut event = make_event("/test");
        event.set_callback(|_| Box::pin(async {}));

        let _ = bot_ref.tell(DispatchEvent { event }).await;
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
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        // Use a catch-all route (no exact match) to test that PluginActor
        // still checks the handler's rule on /skip
        let plugin: Arc<dyn Plugin> = Arc::new(SelectivePlugin {
            meta: PluginMetadata { id: "selective".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("selective", RouteHint::Fallback, Some(is_fullmatch(["/run"])), Arc::new(move |_: HandlerContext| {
                let f = Arc::clone(&called_clone);
                Box::pin(async move { f.store(true, Ordering::SeqCst); Ok(()) })
            }))],
        });
        let plugin_for_bot = Arc::clone(&plugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let bot_ref = Bot::spawn(Bot::new(adapter, vec![(plugin_ref, plugin_for_bot)], ctx, Arc::new(Telemetry::new())));

        let mut event = make_event("/skip");
        event.set_callback(|_| Box::pin(async {}));

        let _ = bot_ref.tell(DispatchEvent { event }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        assert!(!called.load(Ordering::SeqCst), "handler should not be called when rule doesn't match");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_9_dispatch_reply_multi_segment() -> anyhow::Result<()> {
        let recorded = Arc::new(tokio::sync::Mutex::new(String::new()));
        let recorded_clone = Arc::clone(&recorded);

        struct RecordAdapter(Arc<tokio::sync::Mutex<String>>);
        #[async_trait]
        impl BaseAdapter for RecordAdapter {
            fn set_callback(&self, _: Box<dyn Fn(MessageEvent) + Send + Sync>) {}
            async fn send(&self, _target: &str, msg: &MessageChain, _cid: Option<&str>) -> Result<()> {
                let mut guard = self.0.lock().await;
                *guard = msg.summary();
                Ok(())
            }
            async fn run(&self) -> Result<()> { Ok(()) }
        }

        let adapter: Arc<dyn BaseAdapter> = Arc::new(RecordAdapter(recorded_clone));
        let ctx = Arc::new(Ctx::new());

        struct MultiReplyPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for MultiReplyPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(MultiReplyPlugin {
            meta: PluginMetadata { id: "multi_reply".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("multi_reply", RouteHint::Fallback, None, Arc::new(|cx: HandlerContext| {
                Box::pin(async move {
                    let _ = cx.event.reply(MessageSegment::text("multi segment reply")).await;
                    Ok(())
                })
            }))],
        });
        let plugin_for_bot = Arc::clone(&plugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let bot_ref = Bot::spawn(Bot::new(adapter, vec![(plugin_ref, plugin_for_bot)], ctx, Arc::new(Telemetry::new())));

        let event = make_event("/test");
        let _ = bot_ref.tell(DispatchEvent { event }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let guard = recorded.lock().await;
        assert_eq!(*guard, "multi segment reply");
        Ok(())
    }
}
