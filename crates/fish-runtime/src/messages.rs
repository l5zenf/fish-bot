use std::sync::Arc;
use kameo::message::{Context, Message};

use fish_core::ctx::Ctx;
use fish_core::event::{MessageEvent, SystemEvent};
use fish_core::telemetry::Telemetry;

use crate::actor::PluginActor;
use crate::{BaseAdapter, EventHandlerContext, EventHandlerFunc, MessageHandler, RouteHint};

/// Handle a message event — fanned out by BotActor with shared deps.
/// When `handler_id` is set, only that specific handler is executed
/// and rule checking is skipped (Bot already verified via routing table).
/// When `handler_id` is None, all handlers are scanned with rule checks (fallback).
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

        // Determine which handlers to execute based on routing mode.
        // Bot-routed (handler_id set): find by index, skip rule check.
        // Fallback (handler_id=None): scan all handlers with rule checks.
        let handlers = self.plugin().message_handlers();

        let matched_handlers: Vec<&MessageHandler> = match &msg.handler_id {
            Some(hid) => {
                // Bot-routed to a specific handler
                match self
                    .handler_index()
                    .get(hid)
                    .and_then(|&idx| handlers.get(idx))
                {
                    Some(handler) => {
                        // For exact/prefix/keyword routes, Bot verified the match — skip rule.
                        // For regex/fallback routes, Bot cannot pre-filter — check the rule.
                        let matched = match handler.route {
                            RouteHint::Exact(_) | RouteHint::Prefix(_) | RouteHint::Keyword(_) => {
                                true
                            }
                            RouteHint::Regex | RouteHint::Fallback => match &handler.rule {
                                Some(rule) => rule.check(&msg.event),
                                None => true,
                            },
                        };
                        if matched { vec![handler] } else { vec![] }
                    }
                    None => vec![],
                }
            }
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

/// Handle a system event — dispatched by Bot for non-chat business events.
/// Routes to the plugin's event_handlers by matching event_type.
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
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use fish_core::AdapterEventSink;
    use fish_core::message::MessageChain;
    use tokio::sync::Notify;

    use crate::{
        BaseAdapter, EventContext, EventHandler, EventHandlerContext, Plugin, PluginMetadata,
        QueueStrategy, Result, RuntimeConfig,
    };
    use kameo::actor::Spawn;
    use crate::plugin;

    struct MockAdapter;

    #[async_trait]
    impl BaseAdapter for MockAdapter {
        async fn send(
            &self,
            _target_id: &str,
            _message: &MessageChain,
            _cid: Option<&str>,
        ) -> Result<()> {
            Ok(())
        }

        async fn run(&self, _sink: Arc<dyn AdapterEventSink>) -> Result<()> {
            Ok(())
        }
    }

    static SAW_RUNTIME_TELEMETRY: AtomicBool = AtomicBool::new(false);

    struct EventTelemetryPlugin;

    #[plugin]
    impl EventTelemetryPlugin {
        #[event("order_create")]
        async fn on_order(&self, ctx: EventContext) -> Result<()> {
            if ctx.telemetry().handler_started.load(Ordering::SeqCst) > 0 {
                SAW_RUNTIME_TELEMETRY.store(true, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t6_1_system_event_context_uses_runtime_telemetry() -> anyhow::Result<()> {
        SAW_RUNTIME_TELEMETRY.store(false, Ordering::SeqCst);

        let plugin: Arc<dyn Plugin> = Arc::new(EventTelemetryPlugin);
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));
        let telemetry = Arc::new(Telemetry::new());

        let _ = actor_ref
            .tell(HandleSystemEvent {
                event: Arc::new(SystemEvent::new("order_create", serde_json::json!({}))),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: Some("on_order".into()),
                telemetry,
            })
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(SAW_RUNTIME_TELEMETRY.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t6_2_system_event_honors_plugin_timeout() -> anyhow::Result<()> {
        struct TimeoutPlugin {
            meta: PluginMetadata,
            handlers: Vec<EventHandler>,
        }

        impl Plugin for TimeoutPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }

            fn event_handlers(&self) -> &[EventHandler] {
                &self.handlers
            }

            fn runtime_config(&self) -> RuntimeConfig {
                RuntimeConfig {
                    concurrency: 1,
                    timeout: Duration::from_millis(20),
                    queue_strategy: QueueStrategy::DropNewest,
                }
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(TimeoutPlugin {
            meta: PluginMetadata {
                id: "timeout_event".into(),
                ..Default::default()
            },
            handlers: vec![EventHandler::new(
                "order_create",
                "slow_event",
                Arc::new(|_: EventHandlerContext| {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        Ok(())
                    })
                }),
            )],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));
        let telemetry = Arc::new(Telemetry::new());

        let _ = actor_ref
            .tell(HandleSystemEvent {
                event: Arc::new(SystemEvent::new("order_create", serde_json::json!({}))),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: Some("slow_event".into()),
                telemetry: Arc::clone(&telemetry),
            })
            .await;

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(telemetry.handler_timed_out.load(Ordering::SeqCst), 1);
        assert_eq!(telemetry.handler_succeeded.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t6_3_system_event_respects_drop_newest_strategy() -> anyhow::Result<()> {
        struct BusyEventPlugin {
            meta: PluginMetadata,
            handlers: Vec<EventHandler>,
        }

        impl Plugin for BusyEventPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }

            fn event_handlers(&self) -> &[EventHandler] {
                &self.handlers
            }

            fn runtime_config(&self) -> RuntimeConfig {
                RuntimeConfig {
                    concurrency: 1,
                    timeout: Duration::from_secs(1),
                    queue_strategy: QueueStrategy::DropNewest,
                }
            }
        }

        let started = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let handler_started = Arc::clone(&started);
        let handler_finished = Arc::clone(&finished);
        let handler_notify = Arc::clone(&notify);
        let plugin: Arc<dyn Plugin> = Arc::new(BusyEventPlugin {
            meta: PluginMetadata {
                id: "busy_event".into(),
                ..Default::default()
            },
            handlers: vec![EventHandler::new(
                "order_create",
                "busy_event",
                Arc::new(move |_: EventHandlerContext| {
                    let started = Arc::clone(&handler_started);
                    let finished = Arc::clone(&handler_finished);
                    let notify = Arc::clone(&handler_notify);
                    Box::pin(async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        notify.notified().await;
                        finished.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }),
            )],
        });
        let actor_ref = PluginActor::spawn(PluginActor::new(plugin));
        let telemetry = Arc::new(Telemetry::new());

        let _ = actor_ref
            .tell(HandleSystemEvent {
                event: Arc::new(SystemEvent::new("order_create", serde_json::json!({"n": 1}))),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: Some("busy_event".into()),
                telemetry: Arc::clone(&telemetry),
            })
            .await;

        tokio::time::sleep(Duration::from_millis(20)).await;

        let _ = actor_ref
            .tell(HandleSystemEvent {
                event: Arc::new(SystemEvent::new("order_create", serde_json::json!({"n": 2}))),
                adapter: Arc::new(MockAdapter),
                ctx: Arc::new(Ctx::new()),
                handler_id: Some("busy_event".into()),
                telemetry: Arc::clone(&telemetry),
            })
            .await;

        tokio::time::sleep(Duration::from_millis(20)).await;
        notify.notify_waiters();
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(finished.load(Ordering::SeqCst), 1);
        assert_eq!(telemetry.drop_newest_drops.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
