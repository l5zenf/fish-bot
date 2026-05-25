use std::sync::Arc;
use std::time::Duration;

use kameo::actor::{ActorRef, Spawn};
use kameo::mailbox;
use kameo::message::Message as KameoMessage;
use kameo::Actor;
use tokio::sync::OnceCell;

use crate::{
    AppError, Capability, EventContext, EventHandler, EventHandlerFunc, HandlerContext,
    HandlerFunc, MessageContext, MessageHandler, Plugin, PluginMetadata, QueueStrategy,
    RuntimeConfig,
};

#[derive(Clone, Debug)]
pub enum ActorMailbox {
    Bounded(usize),
    Unbounded,
}

impl Default for ActorMailbox {
    fn default() -> Self {
        Self::Bounded(64)
    }
}

struct ActorRuntime<A: Actor> {
    actor_ref: OnceCell<ActorRef<A>>,
    actor_factory: Arc<dyn Fn() -> A + Send + Sync>,
    mailbox: ActorMailbox,
}

impl<A> ActorRuntime<A>
where
    A: Actor<Args = A> + Spawn + Send + Sync + 'static,
{
    fn new(
        actor_factory: Arc<dyn Fn() -> A + Send + Sync>,
        mailbox: ActorMailbox,
    ) -> Self {
        Self {
            actor_ref: OnceCell::new(),
            actor_factory,
            mailbox,
        }
    }

    async fn actor_ref(&self) -> ActorRef<A> {
        self.actor_ref
            .get_or_init(|| async {
                let actor = (self.actor_factory)();
                match self.mailbox.clone() {
                    ActorMailbox::Bounded(capacity) => {
                        A::spawn_with_mailbox(actor, mailbox::bounded(capacity))
                    }
                    ActorMailbox::Unbounded => A::spawn_with_mailbox(actor, mailbox::unbounded()),
                }
            })
            .await
            .clone()
    }
}

pub struct ActorPluginBuilder<A: Actor> {
    metadata: PluginMetadata,
    capabilities: Vec<Capability>,
    runtime: RuntimeConfig,
    mailbox: ActorMailbox,
    actor_factory: Arc<dyn Fn() -> A + Send + Sync>,
    actor_runtime: Arc<ActorRuntime<A>>,
    message_handlers: Vec<MessageHandler>,
    event_handlers: Vec<EventHandler>,
}

impl<A> ActorPluginBuilder<A>
where
    A: Actor<Args = A> + Spawn + Send + Sync + 'static,
{
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        actor_factory: impl Fn() -> A + Send + Sync + 'static,
    ) -> Self {
        let actor_factory: Arc<dyn Fn() -> A + Send + Sync> = Arc::new(actor_factory);
        let mailbox = ActorMailbox::default();
        Self {
            metadata: PluginMetadata {
                id: id.into(),
                name: name.into(),
                ..Default::default()
            },
            capabilities: Vec::new(),
            runtime: RuntimeConfig::default(),
            mailbox: mailbox.clone(),
            actor_runtime: Arc::new(ActorRuntime::new(Arc::clone(&actor_factory), mailbox.clone())),
            actor_factory,
            message_handlers: Vec::new(),
            event_handlers: Vec::new(),
        }
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.metadata.description = desc.into();
        self
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.metadata.version = version.into();
        self
    }

    pub fn author(mut self, author: impl Into<String>) -> Self {
        self.metadata.author = author.into();
        self
    }

    pub fn capability(mut self, capability: Capability) -> Self {
        self.capabilities.push(capability);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.runtime.timeout = timeout;
        self
    }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.runtime.concurrency = concurrency;
        self
    }

    pub fn queue_strategy(mut self, strategy: QueueStrategy) -> Self {
        self.runtime.queue_strategy = strategy;
        self
    }

    pub fn mailbox(mut self, mailbox: ActorMailbox) -> Self {
        self.actor_runtime = Arc::new(ActorRuntime::new(
            Arc::clone(&self.actor_factory),
            mailbox.clone(),
        ));
        self.mailbox = mailbox;
        self
    }

    pub fn on_message<M, F>(
        mut self,
        handler_id: impl Into<String>,
        pattern: impl Into<String>,
        mapper: F,
    ) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + 'static,
        F: Fn(MessageContext) -> M + Send + Sync + 'static,
    {
        let pattern = pattern.into();
        let runtime = Arc::clone(&self.actor_runtime);
        let mapper = Arc::new(mapper);
        let func: HandlerFunc = Arc::new(move |cx: HandlerContext| {
            let runtime = Arc::clone(&runtime);
            let mapper = Arc::clone(&mapper);
            Box::pin(async move {
                let actor_ref = runtime.actor_ref().await;
                let msg = mapper(MessageContext::new(cx.event, cx.adapter, cx.app_ctx, cx.telemetry));
                actor_ref
                    .ask(msg)
                    .await
                    .map_err(|err| AppError::internal(format!("actor ask failed: {err}")))
            })
        });
        let mut handler = MessageHandler::exact(handler_id, vec![&pattern], func);
        handler.timeout = self.runtime.timeout;
        self.message_handlers.push(handler);
        self
    }

    pub fn on_prefix<M, F>(
        mut self,
        handler_id: impl Into<String>,
        prefix: impl Into<String>,
        mapper: F,
    ) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + 'static,
        F: Fn(MessageContext) -> M + Send + Sync + 'static,
    {
        let prefix = prefix.into();
        let runtime = Arc::clone(&self.actor_runtime);
        let mapper = Arc::new(mapper);
        let func: HandlerFunc = Arc::new(move |cx: HandlerContext| {
            let runtime = Arc::clone(&runtime);
            let mapper = Arc::clone(&mapper);
            Box::pin(async move {
                let actor_ref = runtime.actor_ref().await;
                let msg = mapper(MessageContext::new(cx.event, cx.adapter, cx.app_ctx, cx.telemetry));
                actor_ref
                    .ask(msg)
                    .await
                    .map_err(|err| AppError::internal(format!("actor ask failed: {err}")))
            })
        });
        let mut handler = MessageHandler::prefix(handler_id, vec![&prefix], func);
        handler.timeout = self.runtime.timeout;
        self.message_handlers.push(handler);
        self
    }

    pub fn on_keyword<M, F>(
        mut self,
        handler_id: impl Into<String>,
        keyword: impl Into<String>,
        mapper: F,
    ) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + 'static,
        F: Fn(MessageContext) -> M + Send + Sync + 'static,
    {
        let keyword = keyword.into();
        let runtime = Arc::clone(&self.actor_runtime);
        let mapper = Arc::new(mapper);
        let func: HandlerFunc = Arc::new(move |cx: HandlerContext| {
            let runtime = Arc::clone(&runtime);
            let mapper = Arc::clone(&mapper);
            Box::pin(async move {
                let actor_ref = runtime.actor_ref().await;
                let msg = mapper(MessageContext::new(cx.event, cx.adapter, cx.app_ctx, cx.telemetry));
                actor_ref
                    .ask(msg)
                    .await
                    .map_err(|err| AppError::internal(format!("actor ask failed: {err}")))
            })
        });
        let mut handler = MessageHandler::keyword(handler_id, vec![&keyword], func);
        handler.timeout = self.runtime.timeout;
        self.message_handlers.push(handler);
        self
    }

    pub fn on_regex<M, F>(
        mut self,
        handler_id: impl Into<String>,
        pattern: impl Into<String>,
        mapper: F,
    ) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + 'static,
        F: Fn(MessageContext) -> M + Send + Sync + 'static,
    {
        let pattern = pattern.into();
        let runtime = Arc::clone(&self.actor_runtime);
        let mapper = Arc::new(mapper);
        let func: HandlerFunc = Arc::new(move |cx: HandlerContext| {
            let runtime = Arc::clone(&runtime);
            let mapper = Arc::clone(&mapper);
            Box::pin(async move {
                let actor_ref = runtime.actor_ref().await;
                let msg = mapper(MessageContext::new(cx.event, cx.adapter, cx.app_ctx, cx.telemetry));
                actor_ref
                    .ask(msg)
                    .await
                    .map_err(|err| AppError::internal(format!("actor ask failed: {err}")))
            })
        });
        let mut handler = MessageHandler::regex(handler_id, &pattern, func);
        handler.timeout = self.runtime.timeout;
        self.message_handlers.push(handler);
        self
    }

    pub fn on_fallback<M, F>(mut self, handler_id: impl Into<String>, mapper: F) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + 'static,
        F: Fn(MessageContext) -> M + Send + Sync + 'static,
    {
        let runtime = Arc::clone(&self.actor_runtime);
        let mapper = Arc::new(mapper);
        let func: HandlerFunc = Arc::new(move |cx: HandlerContext| {
            let runtime = Arc::clone(&runtime);
            let mapper = Arc::clone(&mapper);
            Box::pin(async move {
                let actor_ref = runtime.actor_ref().await;
                let msg = mapper(MessageContext::new(cx.event, cx.adapter, cx.app_ctx, cx.telemetry));
                actor_ref
                    .ask(msg)
                    .await
                    .map_err(|err| AppError::internal(format!("actor ask failed: {err}")))
            })
        });
        let mut handler = MessageHandler::fallback(handler_id, func);
        handler.timeout = self.runtime.timeout;
        self.message_handlers.push(handler);
        self
    }

    pub fn on_event<M, F>(
        mut self,
        event_type: impl Into<String>,
        handler_id: impl Into<String>,
        mapper: F,
    ) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + 'static,
        F: Fn(EventContext) -> M + Send + Sync + 'static,
    {
        let runtime = Arc::clone(&self.actor_runtime);
        let mapper = Arc::new(mapper);
        let func: EventHandlerFunc = Arc::new(move |cx| {
            let runtime = Arc::clone(&runtime);
            let mapper = Arc::clone(&mapper);
            Box::pin(async move {
                let actor_ref = runtime.actor_ref().await;
                let msg = mapper(EventContext::new(cx.event, cx.adapter, cx.app_ctx, cx.telemetry));
                actor_ref
                    .ask(msg)
                    .await
                    .map_err(|err| AppError::internal(format!("actor ask failed: {err}")))
            })
        });
        self.event_handlers
            .push(EventHandler::new(event_type, handler_id, func));
        self
    }

    pub fn build(self) -> ActorPlugin<A> {
        ActorPlugin {
            metadata: self.metadata,
            capabilities: self.capabilities,
            runtime: self.runtime,
            mailbox: self.mailbox.clone(),
            actor_factory: Arc::clone(&self.actor_factory),
            message_handlers: self.message_handlers,
            event_handlers: self.event_handlers,
            runtime_state: self.actor_runtime,
        }
    }
}

pub struct ActorPlugin<A: Actor> {
    metadata: PluginMetadata,
    capabilities: Vec<Capability>,
    runtime: RuntimeConfig,
    mailbox: ActorMailbox,
    actor_factory: Arc<dyn Fn() -> A + Send + Sync>,
    message_handlers: Vec<MessageHandler>,
    event_handlers: Vec<EventHandler>,
    runtime_state: Arc<ActorRuntime<A>>,
}

impl<A> ActorPlugin<A>
where
    A: Actor<Args = A> + Spawn + Send + Sync + 'static,
{
    pub async fn actor_ref(&self) -> ActorRef<A> {
        self.runtime_state.actor_ref().await
    }

    pub fn mailbox(&self) -> &ActorMailbox {
        &self.mailbox
    }

    pub fn actor_factory(&self) -> &Arc<dyn Fn() -> A + Send + Sync> {
        &self.actor_factory
    }
}

impl<A> Plugin for ActorPlugin<A>
where
    A: Actor<Args = A> + Spawn + Send + Sync + 'static,
{
    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }

    fn message_handlers(&self) -> &[MessageHandler] {
        &self.message_handlers
    }

    fn event_handlers(&self) -> &[EventHandler] {
        &self.event_handlers
    }

    fn capabilities(&self) -> &[Capability] {
        &self.capabilities
    }

    fn runtime_config(&self) -> RuntimeConfig {
        self.runtime.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BaseAdapter;
    use async_trait::async_trait;
    use fish_core::AdapterEventSink;
    use fish_core::message::MessageChain;
    use std::convert::Infallible;
    use std::sync::Arc;
    use kameo::message::Context;

    struct NoopAdapter;

    #[async_trait]
    impl BaseAdapter for NoopAdapter {
        async fn send(
            &self,
            _target_id: &str,
            _message: &MessageChain,
            _cid: Option<&str>,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn run(&self, _sink: Arc<dyn AdapterEventSink>) -> crate::Result<()> {
            Ok(())
        }
    }

    struct CountActor {
        seen: usize,
    }

    impl Actor for CountActor {
        type Args = Self;
        type Error = Infallible;

        async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(args)
        }
    }

    struct Ping(MessageContext);
    struct Seen;
    struct OrderCreated(EventContext);

    impl KameoMessage<Ping> for CountActor {
        type Reply = crate::Result<()>;

        async fn handle(
            &mut self,
            msg: Ping,
            _ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            self.seen += 1;
            msg.0.reply("pong").await
        }
    }

    impl KameoMessage<OrderCreated> for CountActor {
        type Reply = crate::Result<()>;

        async fn handle(
            &mut self,
            msg: OrderCreated,
            _ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            let _ = msg.0.event_type();
            self.seen += 1;
            Ok(())
        }
    }

    impl KameoMessage<Seen> for CountActor {
        type Reply = usize;

        async fn handle(
            &mut self,
            _msg: Seen,
            _ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            self.seen
        }
    }

    #[tokio::test]
    async fn actor_plugin_routes_message_into_actor() -> anyhow::Result<()> {
        let plugin = ActorPluginBuilder::new("count", "Count", || CountActor { seen: 0 })
            .on_message("ping", "/ping", Ping)
            .build();

        let handler = &plugin.message_handlers()[0];
        (handler.func)(HandlerContext::__new(
            fish_core::event::MessageEvent::new(
                "cid".into(),
                "user".into(),
                "User".into(),
                "/ping".into(),
                serde_json::json!({}),
            ),
            Arc::new(NoopAdapter),
            Arc::new(fish_core::ctx::Ctx::new()),
            Arc::new(fish_core::telemetry::Telemetry::new()),
            None,
        ))
        .await?;

        let actor_ref = plugin.actor_ref().await;
        assert_eq!(actor_ref.ask(Seen).await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn actor_plugin_routes_event_into_actor() -> anyhow::Result<()> {
        let plugin = ActorPluginBuilder::new("count", "Count", || CountActor { seen: 0 })
            .on_event("order_create", "order_created", OrderCreated)
            .build();

        let handler = &plugin.event_handlers()[0];
        (handler.func)(crate::EventHandlerContext::__new(
            Arc::new(fish_core::event::SystemEvent::new(
                "order_create",
                serde_json::json!({}),
            )),
            Arc::new(NoopAdapter),
            Arc::new(fish_core::ctx::Ctx::new()),
            Arc::new(fish_core::telemetry::Telemetry::new()),
            None,
        ))
        .await?;

        let actor_ref = plugin.actor_ref().await;
        assert_eq!(actor_ref.ask(Seen).await?, 1);
        Ok(())
    }

    #[test]
    fn actor_plugin_builder_preserves_runtime_config() {
        let plugin = ActorPluginBuilder::new("count", "Count", || CountActor { seen: 0 })
            .timeout(Duration::from_secs(9))
            .concurrency(8)
            .queue_strategy(QueueStrategy::DropOldest(32))
            .mailbox(ActorMailbox::Unbounded)
            .build();

        assert_eq!(plugin.runtime_config().timeout, Duration::from_secs(9));
        assert_eq!(plugin.runtime_config().concurrency, 8);
        assert_eq!(
            plugin.runtime_config().queue_strategy,
            QueueStrategy::DropOldest(32)
        );
        assert!(matches!(plugin.mailbox(), ActorMailbox::Unbounded));
    }
}
