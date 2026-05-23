use std::sync::Arc;

use kameo::prelude::*;
use kameo::message::{Context, Message};

use crate::adapter::BaseAdapter;
use crate::ctx::Ctx;
use crate::event::MessageEvent;
use crate::message::{MessageChain, MessageSegment};
use crate::plugin::actor::{HandleEvent, PluginActor};

/// Bot actor — receives MessageEvents and fans out to all PluginActors.
/// Matching Python bot.py Bot class, powered by kameo actor runtime.
#[derive(Actor)]
pub struct Bot {
    adapter: Arc<dyn BaseAdapter>,
    plugin_refs: Vec<ActorRef<PluginActor>>,
    ctx: Arc<Ctx>,
}

impl Bot {
    pub fn new(
        adapter: Arc<dyn BaseAdapter>,
        plugin_refs: Vec<ActorRef<PluginActor>>,
        ctx: Arc<Ctx>,
    ) -> Self {
        Self {
            adapter,
            plugin_refs,
            ctx,
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
        // Create reply callback bound to this event's sender
        let reply_adapter = Arc::clone(&self.adapter);
        let reply_cid = msg.event.cid.clone();
        let reply_target = msg.event.sender_id.clone();

        let mut event = msg.event;
        event.set_callback(move |reply_msg: MessageSegment| {
            let adapter = Arc::clone(&reply_adapter);
            let target = reply_target.clone();
            let cid = reply_cid.clone();
            Box::pin(async move {
                let chain = MessageChain::from(reply_msg);
                if let Err(e) = adapter.send(&target, &chain, Some(&cid)).await {
                    tracing::error!("Failed to send reply: {}", e);
                }
            })
        });

        let adapter = Arc::clone(&self.adapter);
        let ctx = Arc::clone(&self.ctx);

        // Fan out to all plugin actors — each checks its own rules in isolation
        for plugin_ref in &self.plugin_refs {
            let _ = plugin_ref
                .tell(HandleEvent {
                    event: event.clone(),
                    adapter: Arc::clone(&adapter),
                    ctx: Arc::clone(&ctx),
                })
                .await;
        }
    }
}
