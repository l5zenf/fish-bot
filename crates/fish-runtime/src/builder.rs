use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    Capability, EventHandler, EventHandlerFunc, HandlerFunc, MessageHandler, Plugin,
    PluginMetadata, QueueStrategy, RuntimeConfig,
};

/// Chain-building API for constructing a plugin without boilerplate.
///
/// ```
/// use std::sync::Arc;
/// use fish_runtime::prelude::*;
///
/// let plugin = PluginBuilder::new("demo", "Demo")
///     .description("A demo plugin")
///     .author("me")
///     .command("ping", "/ping", Arc::new(|cx: HandlerContext| {
///         Box::pin(async move {
///             cx.reply(MessageSegment::text("pong!")).await?;
///             Ok(())
///         })
///     }))
///     .build();
///
/// assert_eq!(plugin.metadata().id, "demo");
/// ```
pub struct PluginBuilder {
    metadata: PluginMetadata,
    message_handlers: Vec<MessageHandler>,
    event_handlers: Vec<EventHandler>,
    default_timeout: Duration,
    capabilities: Vec<Capability>,
    concurrency: usize,
    queue_strategy: QueueStrategy,
    plugin_initial_state: Option<crate::PluginState>,
}

impl PluginBuilder {
    /// Start building a plugin with the given id and display name.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            metadata: PluginMetadata {
                id: id.into(),
                name: name.into(),
                ..Default::default()
            },
            message_handlers: Vec::new(),
            event_handlers: Vec::new(),
            default_timeout: Duration::from_secs(5),
            capabilities: Vec::new(),
            concurrency: 64,
            queue_strategy: QueueStrategy::default(),
            plugin_initial_state: None,
        }
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.metadata.description = desc.into();
        self
    }

    pub fn version(mut self, v: impl Into<String>) -> Self {
        self.metadata.version = v.into();
        self
    }

    pub fn author(mut self, a: impl Into<String>) -> Self {
        self.metadata.author = a.into();
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.default_timeout = d;
        self
    }

    /// Add a capability declaration for this plugin.
    pub fn capability(mut self, cap: Capability) -> Self {
        self.capabilities.push(cap);
        self
    }

    /// Set the maximum concurrent handler executions (semaphore permits).
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }

    /// Set the queue strategy.
    pub fn queue_strategy(mut self, s: QueueStrategy) -> Self {
        self.queue_strategy = s;
        self
    }

    /// Set the plugin's initial state (for stateful plugins).
    pub fn state<T: Any + Send + Sync>(mut self, initial: T) -> Self {
        self.plugin_initial_state = Some(Arc::new(tokio::sync::RwLock::new(initial)));
        self
    }

    /// Register an exact-match command (e.g. "/ping").
    pub fn command(
        mut self,
        id: impl Into<String>,
        pattern: impl Into<String>,
        func: HandlerFunc,
    ) -> Self {
        let pattern = pattern.into();
        let mut handler = MessageHandler::exact(id, vec![&pattern], func);
        handler.timeout = self.default_timeout;
        self.message_handlers.push(handler);
        self
    }

    /// Register a prefix-match command (e.g. "/admin").
    pub fn prefix(
        mut self,
        id: impl Into<String>,
        prefix: impl Into<String>,
        func: HandlerFunc,
    ) -> Self {
        let pfx = prefix.into();
        let mut handler = MessageHandler::prefix(id, vec![&pfx], func);
        handler.timeout = self.default_timeout;
        self.message_handlers.push(handler);
        self
    }

    /// Register a keyword-match handler.
    pub fn keyword(
        mut self,
        id: impl Into<String>,
        keyword: impl Into<String>,
        func: HandlerFunc,
    ) -> Self {
        let kw = keyword.into();
        let mut handler = MessageHandler::keyword(id, vec![&kw], func);
        handler.timeout = self.default_timeout;
        self.message_handlers.push(handler);
        self
    }

    /// Register a regex-match handler.
    pub fn regex(
        mut self,
        id: impl Into<String>,
        pattern: impl Into<String>,
        func: HandlerFunc,
    ) -> Self {
        let p = pattern.into();
        let mut handler = MessageHandler::regex(id, &p, func);
        handler.timeout = self.default_timeout;
        self.message_handlers.push(handler);
        self
    }

    /// Register a catch-all (fallback) handler.
    pub fn fallback(mut self, id: impl Into<String>, func: HandlerFunc) -> Self {
        let mut handler = MessageHandler::fallback(id, func);
        handler.timeout = self.default_timeout;
        self.message_handlers.push(handler);
        self
    }

    /// Register a system event handler.
    pub fn on_event(
        mut self,
        event_type: impl Into<String>,
        handler_id: impl Into<String>,
        func: EventHandlerFunc,
    ) -> Self {
        self.event_handlers
            .push(EventHandler::new(event_type, handler_id, func));
        self
    }

    /// Finalize and return a `BuiltPlugin` that implements `Plugin`.
    pub fn build(self) -> BuiltPlugin {
        let runtime = RuntimeConfig {
            concurrency: self.concurrency,
            timeout: self.default_timeout,
            queue_strategy: self.queue_strategy,
        };
        BuiltPlugin {
            metadata: self.metadata,
            message_handlers: self.message_handlers,
            event_handlers: self.event_handlers,
            capabilities: self.capabilities,
            runtime,
            plugin_initial_state: self.plugin_initial_state,
        }
    }
}

/// A concrete struct that implements `Plugin`, produced by `PluginBuilder`.
pub struct BuiltPlugin {
    metadata: PluginMetadata,
    message_handlers: Vec<MessageHandler>,
    event_handlers: Vec<EventHandler>,
    capabilities: Vec<Capability>,
    runtime: RuntimeConfig,
    plugin_initial_state: Option<crate::PluginState>,
}

impl Plugin for BuiltPlugin {
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

    fn initial_state(&self) -> Option<crate::PluginState> {
        self.plugin_initial_state.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HandlerContext;
    use fish_core::message::MessageChain;
    use std::sync::Arc;

    #[test]
    fn s3_1_builder_single_command() {
        let plugin = PluginBuilder::new("test", "Test")
            .command(
                "ping",
                "/ping",
                Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
            )
            .build();

        assert_eq!(plugin.metadata().id, "test");
        assert_eq!(plugin.metadata().name, "Test");
        assert_eq!(plugin.message_handlers().len(), 1);
        assert_eq!(plugin.message_handlers()[0].id, "ping");
    }

    #[test]
    fn s3_2_builder_event_handler() {
        let plugin = PluginBuilder::new("evt", "Event")
            .on_event(
                "order_create",
                "notify",
                Arc::new(|_| Box::pin(async { Ok(()) })),
            )
            .build();

        let handlers = plugin.event_handlers();
        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].event_type, "order_create");
    }

    #[test]
    fn s3_3_builder_metadata() {
        let plugin = PluginBuilder::new("meta", "MetaTest")
            .description("desc")
            .version("2.0")
            .author("tester")
            .build();

        assert_eq!(plugin.metadata().description, "desc");
        assert_eq!(plugin.metadata().version, "2.0");
        assert_eq!(plugin.metadata().author, "tester");
    }

    #[test]
    fn s3_4_builder_all_handler_types() {
        let plugin = PluginBuilder::new("all", "All")
            .command(
                "c1",
                "/c1",
                Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
            )
            .prefix(
                "c2",
                "/admin",
                Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
            )
            .keyword(
                "c3",
                "alert",
                Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
            )
            .regex(
                "c4",
                r"\d+",
                Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
            )
            .fallback(
                "c5",
                Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
            )
            .build();

        assert_eq!(plugin.message_handlers().len(), 5);
    }

    #[test]
    fn s3_5_builder_returns_composable_plugin() {
        let plugin = PluginBuilder::new("reg", "Reg")
            .command(
                "ping",
                "/ping",
                Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
            )
            .build();

        let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(plugin)];
        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0].metadata().id, "reg");
    }

    #[test]
    fn s3_6_builder_command_reply() {
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let c = Arc::clone(&called);

        let plugin = {
            let c = Arc::clone(&c);
            PluginBuilder::new("reply", "ReplyTest")
                .command(
                    "echo",
                    "/echo",
                    Arc::new(move |_: HandlerContext| {
                        let flag = Arc::clone(&c);
                        Box::pin(async move {
                            flag.store(true, std::sync::atomic::Ordering::SeqCst);
                            Ok(())
                        })
                    }),
                )
                .build()
        };

        assert_eq!(plugin.message_handlers().len(), 1);
        // Test the handler directly
        use fish_core::event::MessageEvent;
        use fish_core::message::MessageChain;
        let event = MessageEvent::new(
            "cid".into(),
            "uid".into(),
            "name".into(),
            MessageChain::from("/echo"),
            serde_json::json!({}),
        );

        let adapter: Arc<dyn crate::BaseAdapter> = Arc::new(TestAdapter);
        let ctx = Arc::new(fish_core::ctx::Ctx::new());
        let telemetry = Arc::new(fish_core::telemetry::Telemetry::new());

        let fut = (plugin.message_handlers()[0].func)(HandlerContext::__new(
            event,
            adapter,
            ctx,
            telemetry,
            None,
        ));
        let _ = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(fut)
            .unwrap();
        assert!(c.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn s5_1_builder_stateful() {
        use tokio::sync::RwLock;
        let plugin = PluginBuilder::new("state", "Stateful")
            .state(42usize)
            .build();

        let state_arc = plugin.initial_state().unwrap();
        let lock = state_arc.downcast_ref::<RwLock<usize>>().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let value = rt.block_on(async { *lock.read().await });
        assert_eq!(value, 42);
    }

    #[test]
    fn s4_1_builder_capability() {
        use crate::Capability;
        let plugin = PluginBuilder::new("cap", "CapTest")
            .capability(Capability::Network)
            .capability(Capability::SendMessage)
            .build();

        assert_eq!(plugin.capabilities().len(), 2);
        assert!(plugin.capabilities().contains(&Capability::Network));
    }

    #[test]
    fn s4_2_builder_runtime_config() {
        use crate::QueueStrategy;
        let plugin = PluginBuilder::new("rt", "RTTest")
            .concurrency(8)
            .timeout(Duration::from_secs(3))
            .queue_strategy(QueueStrategy::DropOldest(10))
            .build();

        let config = plugin.runtime_config();
        assert_eq!(config.concurrency, 8);
        assert_eq!(config.timeout, Duration::from_secs(3));
        assert_eq!(config.queue_strategy, QueueStrategy::DropOldest(10));
    }

    struct TestAdapter;
    #[async_trait::async_trait]
    impl crate::BaseAdapter for TestAdapter {
        async fn send(
            &self,
            _: &str,
            _: &MessageChain,
            _: Option<&str>,
        ) -> fish_core::error::Result<()> {
            Ok(())
        }
        async fn run(
            &self,
            _: std::sync::Arc<dyn fish_core::AdapterEventSink>,
        ) -> fish_core::error::Result<()> {
            Ok(())
        }
    }
}
