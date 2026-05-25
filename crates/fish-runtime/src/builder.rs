use std::sync::{Arc, OnceLock};
use std::time::Duration;

use kameo::Actor;
use kameo::actor::{ActorRef, Spawn};
use kameo::mailbox;
use kameo::message::Message as KameoMessage;
use parking_lot::RwLock;
use tokio::sync::OnceCell;

use crate::handlers::{
    EventHandler, EventHandlerFunc, HandlerContext, HandlerFunc, MessageHandler,
};
use crate::plugin::PluginMetadata;
use crate::runtime::{QueueStrategy, RuntimeConfig};
use crate::{ActorBusHandle, AppError, EventContext, MessageContext, Plugin};

#[derive(Clone, Debug)]
enum MailboxConfig {
    Bounded(usize),
    Unbounded,
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self::Bounded(64)
    }
}

struct ActorRuntime<A: Actor> {
    actor_ref: OnceCell<ActorRef<A>>,
    bridge_ready: OnceCell<()>,
    bridge_installers: RwLock<Vec<BridgeInstaller<A>>>,
    actor_factory: Arc<dyn Fn() -> A + Send + Sync>,
    mailbox: MailboxConfig,
}

type BridgeInstaller<A> = Arc<dyn Fn(&Arc<ActorRuntime<A>>, ActorBusHandle) + Send + Sync>;

impl<A> ActorRuntime<A>
where
    A: Actor<Args = A> + Spawn + Send + Sync + 'static,
{
    fn new(actor_factory: Arc<dyn Fn() -> A + Send + Sync>, mailbox: MailboxConfig) -> Self {
        Self {
            actor_ref: OnceCell::new(),
            bridge_ready: OnceCell::new(),
            bridge_installers: RwLock::new(Vec::new()),
            actor_factory,
            mailbox,
        }
    }

    async fn actor_ref(&self) -> ActorRef<A> {
        self.actor_ref
            .get_or_init(|| async {
                let actor = (self.actor_factory)();
                match self.mailbox.clone() {
                    MailboxConfig::Bounded(capacity) => {
                        A::spawn_with_mailbox(actor, mailbox::bounded(capacity))
                    }
                    MailboxConfig::Unbounded => A::spawn_with_mailbox(actor, mailbox::unbounded()),
                }
            })
            .await
            .clone()
    }

    fn add_bridge(&self, bridge: BridgeInstaller<A>) {
        self.bridge_installers.write().push(bridge);
    }

    async fn ensure_registered(self: &Arc<Self>, bus: ActorBusHandle) {
        self.bridge_ready
            .get_or_init(|| async {
                let installers = self.bridge_installers.read().clone();
                for installer in installers {
                    installer(self, bus.clone());
                }
            })
            .await;
    }
}

pub struct ActorPluginBuilder<A: Actor> {
    metadata: PluginMetadata,
    runtime: RuntimeConfig,
    mailbox: MailboxConfig,
    actor_factory: Arc<dyn Fn() -> A + Send + Sync>,
    message_bindings: Vec<MessageBinding<A>>,
    event_bindings: Vec<EventBinding<A>>,
    runtime_state: OnceLock<Arc<ActorRuntime<A>>>,
    message_handlers: OnceLock<Vec<MessageHandler>>,
    event_handlers: OnceLock<Vec<EventHandler>>,
}

type MessageBinding<A> =
    Box<dyn Fn(&PluginMetadata, Arc<ActorRuntime<A>>, Duration) -> MessageHandler + Send + Sync>;
type EventBinding<A> =
    Box<dyn Fn(&PluginMetadata, Arc<ActorRuntime<A>>) -> EventHandler + Send + Sync>;

#[derive(Clone)]
enum MessageRouteSpec {
    Exact(String),
    Prefix(String),
    Keyword(String),
    Regex(String),
    Fallback,
}

impl<A> ActorPluginBuilder<A>
where
    A: Actor<Args = A> + Spawn + Send + Sync + 'static,
{
    pub fn new(actor_factory: impl Fn() -> A + Send + Sync + 'static) -> Self {
        let actor_factory: Arc<dyn Fn() -> A + Send + Sync> = Arc::new(actor_factory);
        let mailbox = MailboxConfig::default();
        let name = type_label::<A>();
        Self {
            metadata: PluginMetadata {
                id: to_snake_case(&name),
                name,
            },
            runtime: RuntimeConfig::default(),
            mailbox,
            actor_factory,
            message_bindings: Vec::new(),
            event_bindings: Vec::new(),
            runtime_state: OnceLock::new(),
            message_handlers: OnceLock::new(),
            event_handlers: OnceLock::new(),
        }
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.metadata.id = id.into();
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.metadata.name = name.into();
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

    pub fn bounded_mailbox(mut self, capacity: usize) -> Self {
        self.mailbox = MailboxConfig::Bounded(capacity);
        self
    }

    pub fn unbounded_mailbox(mut self) -> Self {
        self.mailbox = MailboxConfig::Unbounded;
        self
    }

    pub fn on_message<M, F>(self, pattern: impl Into<String>, mapper: F) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + Sync + 'static,
        F: Fn(MessageContext) -> M + Send + Sync + 'static,
    {
        self.register_message_handler::<M, F>(MessageRouteSpec::Exact(pattern.into()), mapper)
    }

    pub fn on_prefix<M, F>(self, prefix: impl Into<String>, mapper: F) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + Sync + 'static,
        F: Fn(MessageContext) -> M + Send + Sync + 'static,
    {
        self.register_message_handler::<M, F>(MessageRouteSpec::Prefix(prefix.into()), mapper)
    }

    pub fn on_keyword<M, F>(self, keyword: impl Into<String>, mapper: F) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + Sync + 'static,
        F: Fn(MessageContext) -> M + Send + Sync + 'static,
    {
        self.register_message_handler::<M, F>(MessageRouteSpec::Keyword(keyword.into()), mapper)
    }

    pub fn on_regex<M, F>(self, pattern: impl Into<String>, mapper: F) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + Sync + 'static,
        F: Fn(MessageContext) -> M + Send + Sync + 'static,
    {
        self.register_message_handler::<M, F>(MessageRouteSpec::Regex(pattern.into()), mapper)
    }

    pub fn on_fallback<M, F>(self, mapper: F) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + Sync + 'static,
        F: Fn(MessageContext) -> M + Send + Sync + 'static,
    {
        self.register_message_handler::<M, F>(MessageRouteSpec::Fallback, mapper)
    }

    pub fn on_event<M, F>(mut self, event_type: impl Into<String>, mapper: F) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + Sync + 'static,
        F: Fn(EventContext) -> M + Send + Sync + 'static,
    {
        let event_type = event_type.into();
        let handler_id = derived_handler_id::<M>();
        let mapper = Arc::new(mapper);
        self.event_bindings.push(Box::new(move |metadata, runtime| {
            let topic = event_topic(&metadata.id, &handler_id);
            install_message_bridge::<A, M>(&runtime, topic.clone());
            let func = build_event_publisher(topic, Arc::clone(&mapper), Arc::clone(&runtime));
            EventHandler::new(event_type.clone(), handler_id.clone(), func)
        }));
        self
    }

    fn register_message_handler<M, F>(mut self, route: MessageRouteSpec, mapper: F) -> Self
    where
        A: KameoMessage<M, Reply = crate::Result<()>>,
        M: Send + Sync + 'static,
        F: Fn(MessageContext) -> M + Send + Sync + 'static,
    {
        let handler_id = derived_handler_id::<M>();
        let mapper = Arc::new(mapper);
        self.message_bindings
            .push(Box::new(move |metadata, runtime, timeout| {
                let topic = message_topic(&metadata.id, &handler_id);
                install_message_bridge::<A, M>(&runtime, topic.clone());
                let func =
                    build_message_publisher(topic, Arc::clone(&mapper), Arc::clone(&runtime));
                let mut handler = route.clone().into_handler(handler_id.clone(), func);
                handler.timeout = timeout;
                handler
            }));
        self
    }

    fn runtime_state(&self) -> Arc<ActorRuntime<A>> {
        self.runtime_state
            .get_or_init(|| {
                Arc::new(ActorRuntime::new(
                    Arc::clone(&self.actor_factory),
                    self.mailbox.clone(),
                ))
            })
            .clone()
    }

    fn compiled_message_handlers(&self) -> &[MessageHandler] {
        self.message_handlers.get_or_init(|| {
            let runtime = self.runtime_state();
            self.message_bindings
                .iter()
                .map(|binding| binding(&self.metadata, Arc::clone(&runtime), self.runtime.timeout))
                .collect()
        })
    }

    fn compiled_event_handlers(&self) -> &[EventHandler] {
        self.event_handlers.get_or_init(|| {
            let runtime = self.runtime_state();
            self.event_bindings
                .iter()
                .map(|binding| binding(&self.metadata, Arc::clone(&runtime)))
                .collect()
        })
    }
}

impl<A> ActorPluginBuilder<A>
where
    A: Actor<Args = A> + Spawn + Send + Sync + 'static,
{
    pub async fn actor_ref(&self) -> ActorRef<A> {
        self.runtime_state().actor_ref().await
    }
}

impl<A> Plugin for ActorPluginBuilder<A>
where
    A: Actor<Args = A> + Spawn + Send + Sync + 'static,
{
    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }

    fn message_handlers(&self) -> &[MessageHandler] {
        self.compiled_message_handlers()
    }

    fn event_handlers(&self) -> &[EventHandler] {
        self.compiled_event_handlers()
    }

    fn runtime_config(&self) -> RuntimeConfig {
        self.runtime.clone()
    }
}

impl MessageRouteSpec {
    fn into_handler(self, handler_id: String, func: HandlerFunc) -> MessageHandler {
        match self {
            Self::Exact(pattern) => MessageHandler::exact(handler_id, vec![pattern.as_str()], func),
            Self::Prefix(prefix) => MessageHandler::prefix(handler_id, vec![prefix.as_str()], func),
            Self::Keyword(keyword) => {
                MessageHandler::keyword(handler_id, vec![keyword.as_str()], func)
            }
            Self::Regex(pattern) => MessageHandler::regex(handler_id, &pattern, func),
            Self::Fallback => MessageHandler::fallback(handler_id, func),
        }
    }
}

fn message_topic(plugin_id: &str, handler_id: &str) -> String {
    format!("plugin:{plugin_id}:message:{handler_id}")
}

fn event_topic(plugin_id: &str, handler_id: &str) -> String {
    format!("plugin:{plugin_id}:event:{handler_id}")
}

fn derived_handler_id<M>() -> String {
    to_snake_case(&type_label::<M>())
}

fn type_label<T>() -> String {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("handler")
        .to_string()
}

fn to_snake_case(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut prev_is_lower_or_digit = false;
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() {
                if prev_is_lower_or_digit && !out.ends_with('_') {
                    out.push('_');
                }
                out.push(ch.to_ascii_lowercase());
                prev_is_lower_or_digit = false;
            } else {
                out.push(ch);
                prev_is_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
            }
        } else if !out.ends_with('_') && !out.is_empty() {
            out.push('_');
            prev_is_lower_or_digit = false;
        }
    }

    out.trim_matches('_').to_string()
}

fn build_message_publisher<A, M, F>(
    topic: String,
    mapper: Arc<F>,
    runtime: Arc<ActorRuntime<A>>,
) -> HandlerFunc
where
    A: Actor<Args = A> + Spawn + Send + Sync + 'static,
    M: Send + Sync + 'static,
    F: Fn(MessageContext) -> M + Send + Sync + 'static,
{
    Arc::new(move |cx: HandlerContext| {
        let topic = topic.clone();
        let mapper = Arc::clone(&mapper);
        let runtime = Arc::clone(&runtime);
        Box::pin(async move {
            let app_ctx = Arc::clone(&cx.app_ctx);
            let message = mapper(MessageContext::new(
                cx.event,
                cx.adapter,
                Arc::clone(&app_ctx),
                cx.telemetry,
            ));
            publish_actor_message(topic, runtime, app_ctx, message).await
        })
    })
}

fn build_event_publisher<A, M, F>(
    topic: String,
    mapper: Arc<F>,
    runtime: Arc<ActorRuntime<A>>,
) -> EventHandlerFunc
where
    A: Actor<Args = A> + Spawn + Send + Sync + 'static,
    M: Send + Sync + 'static,
    F: Fn(EventContext) -> M + Send + Sync + 'static,
{
    Arc::new(move |cx| {
        let topic = topic.clone();
        let mapper = Arc::clone(&mapper);
        let runtime = Arc::clone(&runtime);
        Box::pin(async move {
            let app_ctx = Arc::clone(&cx.app_ctx);
            let message = mapper(EventContext::new(
                cx.event,
                cx.adapter,
                Arc::clone(&app_ctx),
                cx.telemetry,
            ));
            publish_actor_message(topic, runtime, app_ctx, message).await
        })
    })
}

async fn publish_actor_message<A, M>(
    topic: String,
    runtime: Arc<ActorRuntime<A>>,
    app_ctx: Arc<fish_core::ctx::Ctx>,
    message: M,
) -> crate::Result<()>
where
    A: Actor<Args = A> + Spawn + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    let bus = app_ctx
        .get::<ActorBusHandle>()
        .map(|handle| (*handle).clone())
        .ok_or_else(|| AppError::internal("actor bus is unavailable"))?;
    runtime.ensure_registered(bus.clone()).await;
    bus.publish(topic, message).await
}

fn install_message_bridge<A, M>(runtime: &Arc<ActorRuntime<A>>, topic: String)
where
    A: Actor<Args = A> + Spawn + Send + Sync + 'static,
    A: KameoMessage<M, Reply = crate::Result<()>>,
    M: Send + Sync + 'static,
{
    runtime.add_bridge(Arc::new(move |runtime, bus| {
        let topic = topic.clone();
        bus.subscribe::<M, _, _>(topic.clone(), {
            let runtime = Arc::clone(runtime);
            move |payload| {
                let runtime = Arc::clone(&runtime);
                let topic = topic.clone();
                async move {
                    let actor_ref = runtime.actor_ref().await;
                    let message = Arc::try_unwrap(payload).map_err(|_| {
                        AppError::internal(format!(
                            "actor bus topic `{topic}` requires a single subscriber"
                        ))
                    })?;
                    actor_ref
                        .ask(message)
                        .await
                        .map_err(|err| AppError::internal(format!("actor ask failed: {err}")))
                }
            }
        });
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::EventHandlerContext;
    use crate::{ActorBusHandle, BaseAdapter};
    use async_trait::async_trait;
    use fish_core::AdapterEventSink;
    use fish_core::message::MessageChain;
    use kameo::message::Context;
    use std::convert::Infallible;
    use std::sync::Arc;

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

        async fn on_start(
            args: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
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
        let plugin = ActorPluginBuilder::new(|| CountActor { seen: 0 }).on_message("/ping", Ping);

        let app_ctx = Arc::new(fish_core::ctx::Ctx::new());
        app_ctx.insert(ActorBusHandle::runtime_default());

        let handler = &plugin.message_handlers()[0];
        assert_eq!(handler.id, "ping");
        (handler.func)(HandlerContext::__new(
            fish_core::event::MessageEvent::new(
                "cid".into(),
                "user".into(),
                "User".into(),
                "/ping".into(),
                serde_json::json!({}),
            ),
            Arc::new(NoopAdapter),
            app_ctx,
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
        let plugin = ActorPluginBuilder::new(|| CountActor { seen: 0 })
            .on_event("order_create", OrderCreated);

        let app_ctx = Arc::new(fish_core::ctx::Ctx::new());
        app_ctx.insert(ActorBusHandle::runtime_default());

        let handler = &plugin.event_handlers()[0];
        assert_eq!(handler.id, "order_created");
        (handler.func)(EventHandlerContext::__new(
            Arc::new(fish_core::event::SystemEvent::new(
                "order_create",
                serde_json::json!({}),
            )),
            Arc::new(NoopAdapter),
            app_ctx,
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
        let plugin = ActorPluginBuilder::new(|| CountActor { seen: 0 })
            .timeout(Duration::from_secs(9))
            .concurrency(8)
            .queue_strategy(QueueStrategy::DropOldest(32))
            .unbounded_mailbox();

        assert_eq!(plugin.runtime_config().timeout, Duration::from_secs(9));
        assert_eq!(plugin.runtime_config().concurrency, 8);
        assert_eq!(
            plugin.runtime_config().queue_strategy,
            QueueStrategy::DropOldest(32)
        );
    }

    #[test]
    fn actor_plugin_builder_is_itself_a_plugin() {
        let plugin = ActorPluginBuilder::new(|| CountActor { seen: 0 }).on_message("/ping", Ping);

        let plugin: Arc<dyn Plugin> = Arc::new(plugin);
        assert_eq!(plugin.metadata().id, "count_actor");
        assert_eq!(plugin.metadata().name, "CountActor");
        assert_eq!(plugin.message_handlers().len(), 1);
    }

    #[test]
    fn actor_plugin_builder_allows_metadata_override() {
        let plugin = ActorPluginBuilder::new(|| CountActor { seen: 0 })
            .id("count")
            .name("Count")
            .on_message("/ping", Ping);

        assert_eq!(plugin.metadata().id, "count");
        assert_eq!(plugin.metadata().name, "Count");
    }
}
