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
