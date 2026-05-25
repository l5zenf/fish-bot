use std::sync::Arc;

use async_trait::async_trait;
use kameo::actor::ActorRef;
use kameo::actor::Spawn;

use fish_core::AdapterEventSink;
use fish_core::event::{MessageEvent, SystemEvent};

use crate::actor::PluginActor;
use crate::bot::{Bot, DispatchEvent, DispatchSystemEvent};
use crate::{ActorBusHandle, BaseAdapter, Ctx, Plugin, Result, RuntimeActorBus, Telemetry};

pub struct RuntimeHost {
    adapter: Arc<dyn BaseAdapter>,
    bot_ref: ActorRef<Bot>,
}

impl RuntimeHost {
    pub fn new(
        adapter: Arc<dyn BaseAdapter>,
        plugins: Vec<Arc<dyn Plugin>>,
        ctx: Arc<Ctx>,
        telemetry: Arc<Telemetry>,
    ) -> Self {
        if ctx.get::<ActorBusHandle>().is_none() {
            ctx.insert(ActorBusHandle::new(Arc::new(RuntimeActorBus::default())));
        }

        let plugin_refs = plugins
            .into_iter()
            .map(|plugin| {
                let plugin_ref = PluginActor::spawn(PluginActor::new(Arc::clone(&plugin)));
                (plugin_ref, plugin)
            })
            .collect();

        let bot_ref = Bot::spawn(Bot::new(Arc::clone(&adapter), plugin_refs, ctx, telemetry));

        Self { adapter, bot_ref }
    }

    pub fn with_plugins(adapter: Arc<dyn BaseAdapter>, plugins: Vec<Arc<dyn Plugin>>) -> Self {
        Self::new(
            adapter,
            plugins,
            Arc::new(Ctx::new()),
            Arc::new(Telemetry::new()),
        )
    }

    pub fn bot_ref(&self) -> &ActorRef<Bot> {
        &self.bot_ref
    }

    pub async fn run(&self) -> Result<()> {
        let sink: Arc<dyn AdapterEventSink> = Arc::new(BotEventSink {
            bot_ref: self.bot_ref.clone(),
        });
        self.adapter.run(sink).await
    }
}

struct BotEventSink {
    bot_ref: ActorRef<Bot>,
}

#[async_trait]
impl AdapterEventSink for BotEventSink {
    async fn handle_message(&self, event: MessageEvent) -> Result<()> {
        let _ = self.bot_ref.tell(DispatchEvent { event }).await;
        Ok(())
    }

    async fn handle_system(&self, event: SystemEvent) -> Result<()> {
        let _ = self
            .bot_ref
            .tell(DispatchSystemEvent {
                event: Arc::new(event),
            })
            .await;
        Ok(())
    }
}
