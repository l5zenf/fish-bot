use std::sync::Arc;
use std::time::Duration;

use kameo::message::{Context, Message};

use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_core::event::{MessageEvent, SystemEvent};
use fish_core::telemetry::Telemetry;
use fish_plugin::plugin::{EventHandlerFunc, RouteHint};

use crate::actor::PluginActor;

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

        let matched_handlers: Vec<&fish_plugin::plugin::MessageHandler> = match &msg.handler_id {
            Some(hid) => {
                // Bot-routed to a specific handler
                match self.handler_index().get(hid).and_then(|&idx| handlers.get(idx)) {
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
                        if matched {
                            vec![handler]
                        } else {
                            vec![]
                        }
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
            ).await;
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
        let handlers = self.plugin().event_handlers();

        // Collect Arc clones before handlers is dropped (lifetime issue with tokio::spawn)
        let funcs: Vec<EventHandlerFunc> = match &msg.handler_id {
            Some(hid) => handlers
                .values()
                .flatten()
                .filter(|h| &h.id == hid)
                .map(|h| Arc::clone(&h.func))
                .collect(),
            None => handlers
                .get(&msg.event.event_type)
                .map(|v| v.iter().map(|h| Arc::clone(&h.func)).collect())
                .unwrap_or_default(),
        };

        for func in funcs {
            let event = Arc::clone(&msg.event);
            let adapter = Arc::clone(&msg.adapter);
            let ctx = Arc::clone(&msg.ctx);
            let telemetry = Arc::clone(&msg.telemetry);
            let handler_timeout = Duration::from_secs(5);

            telemetry.handler_started.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::spawn(async move {
                let started = std::time::Instant::now();
                let result = tokio::time::timeout(handler_timeout, (func)(event, adapter, ctx)).await;
                match result {
                    Ok(Ok(())) => {
                        telemetry.handler_succeeded.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::debug!(
                            cost_ms = started.elapsed().as_millis(),
                            "system event handler finished"
                        );
                    }
                    Ok(Err(e)) => {
                        telemetry.handler_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::error!(
                            error = %e,
                            cost_ms = started.elapsed().as_millis(),
                            "system event handler failed"
                        );
                    }
                    Err(_) => {
                        telemetry.handler_timed_out.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::warn!(
                            timeout_ms = handler_timeout.as_millis(),
                            "system event handler timeout"
                        );
                    }
                }
            });
        }
    }
}
