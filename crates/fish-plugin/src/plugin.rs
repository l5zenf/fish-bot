use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_core::error::Result;
use fish_core::event::{MessageEvent, SystemEvent};
use fish_core::rule::{is_fullmatch, is_keywords, is_regex, is_startswith, Rule};
use fish_core::telemetry::Telemetry;
pub mod actor;

pub mod echo;

/// Plugin metadata.
#[derive(Debug, Clone)]
pub struct PluginMetadata {
    pub id: String,
    pub name: String,
    pub description: String,
    pub version: String,
    pub author: String,
}

impl Default for PluginMetadata {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            description: String::new(),
            version: "1.0.0".into(),
            author: "Unknown".into(),
        }
    }
}

/// Capabilities a plugin may request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Capability {
    /// Can make outbound HTTP requests.
    Network,
    /// Can read from the local filesystem.
    FileSystem,
    /// Can write to the local filesystem.
    FileSystemWrite,
    /// Can send messages through the adapter.
    SendMessage,
    /// Can read shared application context (Ctx).
    ReadAppContext,
}

/// Per-plugin runtime configuration.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Maximum concurrent handler executions (semaphore permits).
    pub concurrency: usize,
    /// Default timeout for handler execution.
    pub timeout: Duration,
    /// Queue strategy when at capacity.
    pub queue_strategy: QueueStrategy,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            concurrency: 64,
            timeout: Duration::from_secs(5),
            queue_strategy: QueueStrategy::default(),
        }
    }
}

/// Full plugin manifest — metadata + capabilities + runtime config.
#[derive(Debug, Clone)]
pub struct PluginManifest {
    pub metadata: PluginMetadata,
    pub capabilities: Vec<Capability>,
    pub runtime: RuntimeConfig,
}

/// Route hint for Bot-level routing table.
/// Allows Bot to pre-filter messages by text before dispatching to PluginActor.
#[derive(Debug, Clone)]
pub enum RouteHint {
    /// Exact trimmed-text match, e.g. "/ping". Bot looks up in HashMap.
    Exact(Vec<String>),
    /// Text starts with any of these prefixes, e.g. "/admin".
    Prefix(Vec<String>),
    /// Text contains any of these keywords.
    Keyword(Vec<String>),
    /// Regex-based match — Bot cannot pre-filter, always dispatched.
    Regex,
    /// No pre-filter hint — always dispatched (catch-all handlers).
    Fallback,
}

/// Queue strategy when a plugin's handler concurrency limit is reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueStrategy {
    /// Drop new events immediately when at capacity.
    DropNewest,
    /// Keep a bounded queue; drop the oldest queued event when full.
    DropOldest(usize),
}

impl Default for QueueStrategy {
    fn default() -> Self {
        Self::DropNewest
    }
}

/// Context passed to every message handler execution.
/// Carries the event, adapter for replies, and shared application context.
/// New fields (logger, metrics, cancel_token) can be added here without
/// changing the handler function signature.
pub struct HandlerContext {
    pub event: MessageEvent,
    pub adapter: Arc<dyn BaseAdapter>,
    pub app_ctx: Arc<Ctx>,
    pub telemetry: Arc<Telemetry>,
    /// Plugin's mutable state (set for stateful plugins).
    pub plugin_state: Option<Arc<dyn Any + Send + Sync>>,
}

impl HandlerContext {
    /// Access the plugin's mutable state.
    /// Returns None if the plugin is not stateful or if the type doesn't match.
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&parking_lot::RwLock<T>> {
        self.plugin_state
            .as_ref()?
            .downcast_ref::<parking_lot::RwLock<T>>()
    }
}

/// A pinned, boxed future returned by a handler function.
pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// Handler function type — takes a HandlerContext and returns a HandlerFuture.
pub type HandlerFunc = Arc<
    dyn Fn(HandlerContext) -> HandlerFuture + Send + Sync,
>;

/// A message handler registered by a plugin.
pub struct MessageHandler {
    pub id: String,
    pub route: RouteHint,
    pub rule: Option<Rule>,
    pub timeout: Duration,
    pub func: HandlerFunc,
}

impl MessageHandler {
    /// Create a new handler with the given id, route hint, optional rule, and function.
    /// Default timeout is 5 seconds.
    pub fn new(
        id: impl Into<String>,
        route: RouteHint,
        rule: Option<Rule>,
        func: HandlerFunc,
    ) -> Self {
        Self {
            id: id.into(),
            route,
            rule,
            timeout: Duration::from_secs(5),
            func,
        }
    }

    /// Exact-match handler: auto-generates both RouteHint::Exact and is_fullmatch Rule.
    /// Example: `MessageHandler::exact("ping", vec!["/ping"], handler_fn)`
    pub fn exact(id: impl Into<String>, patterns: Vec<&str>, func: HandlerFunc) -> Self {
        let route = RouteHint::Exact(patterns.iter().map(|s| s.to_string()).collect());
        Self {
            id: id.into(),
            route,
            rule: Some(is_fullmatch(patterns)),
            timeout: Duration::from_secs(5),
            func,
        }
    }

    /// Prefix-match handler: auto-generates RouteHint::Prefix and is_startswith Rule.
    pub fn prefix(id: impl Into<String>, prefixes: Vec<&str>, func: HandlerFunc) -> Self {
        let route = RouteHint::Prefix(prefixes.iter().map(|s| s.to_string()).collect());
        Self {
            id: id.into(),
            route,
            rule: Some(is_startswith(prefixes)),
            timeout: Duration::from_secs(5),
            func,
        }
    }

    /// Keyword-match handler: auto-generates RouteHint::Keyword and is_keywords Rule.
    pub fn keyword(id: impl Into<String>, keywords: Vec<&str>, func: HandlerFunc) -> Self {
        let route = RouteHint::Keyword(keywords.iter().map(|s| s.to_string()).collect());
        Self {
            id: id.into(),
            route,
            rule: Some(is_keywords(keywords)),
            timeout: Duration::from_secs(5),
            func,
        }
    }

    /// Regex handler: auto-generates RouteHint::Regex and is_regex Rule.
    /// Bot cannot pre-filter regex — always dispatched to PluginActor for rule check.
    pub fn regex(id: impl Into<String>, pattern: &str, func: HandlerFunc) -> Self {
        Self {
            id: id.into(),
            route: RouteHint::Regex,
            rule: Some(is_regex(pattern)),
            timeout: Duration::from_secs(5),
            func,
        }
    }

    /// Catch-all handler: RouteHint::Fallback, no rule (always executes for every event).
    pub fn fallback(id: impl Into<String>, func: HandlerFunc) -> Self {
        Self {
            id: id.into(),
            route: RouteHint::Fallback,
            rule: None,
            timeout: Duration::from_secs(5),
            func,
        }
    }
}

/// A pinned, boxed future returned by an event handler function.
pub type EventHandlerFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>;

/// Event handler function type — receives SystemEvent + adapter + ctx + plugin_state.
pub type EventHandlerFunc = Arc<
    dyn Fn(
        Arc<SystemEvent>,
        Arc<dyn BaseAdapter>,
        Arc<Ctx>,
        Option<Arc<dyn Any + Send + Sync>>,
    ) -> EventHandlerFuture + Send + Sync,
>;

/// An event handler registered by a plugin.
/// Handles non-message events (notices, business events like trade orders, etc.).
#[derive(Clone)]
pub struct EventHandler {
    pub id: String,
    pub func: EventHandlerFunc,
    pub rule: Option<Rule>,
}

impl EventHandler {
    /// Create a new event handler with the given id and function.
    pub fn new(id: impl Into<String>, func: EventHandlerFunc) -> Self {
        Self {
            id: id.into(),
            func,
            rule: None,
        }
    }
}

/// Plugin trait.
pub trait Plugin: Send + Sync + 'static {
    fn metadata(&self) -> &PluginMetadata;

    /// Message handlers — each handler has a func and an optional Rule.
    fn message_handlers(&self) -> &[MessageHandler] {
        &[]
    }

    /// Event handlers keyed by event type (e.g. "notice", "request", "meta_event").
    fn event_handlers(&self) -> HashMap<String, Vec<EventHandler>> {
        HashMap::new()
    }

    /// Quick-check whether this plugin supports the given event.
    ///
    /// Returns `true` if at least one handler has no rule or has a matching rule.
    /// Used by Bot to skip plugin actors whose rules can't match, avoiding
    /// unnecessary actor dispatch.
    fn supports(&self, event: &MessageEvent) -> bool {
        self.message_handlers().iter().any(|h| match &h.rule {
            Some(rule) => rule.check(event),
            None => true,
        })
    }

    /// Return the full plugin manifest.
    fn manifest(&self) -> PluginManifest {
        PluginManifest {
            metadata: self.metadata().clone(),
            capabilities: Vec::new(),
            runtime: RuntimeConfig::default(),
        }
    }

    /// Return the runtime configuration for this plugin.
    fn runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig::default()
    }

    /// Return declared capabilities.
    fn capabilities(&self) -> &[Capability] {
        &[]
    }

    /// Create initial mutable state for this plugin.
    /// Override in stateful plugins. Returns None for stateless plugins.
    fn initial_state(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
}

/// A plugin that maintains mutable state across handler invocations.
/// State lives inside the PluginActor, one instance per actor.
///
/// Implement this trait instead of `Plugin` for plugins with mutable state.
pub trait StatefulPlugin: Plugin {
    /// The state type this plugin maintains.
    type State: Send + Sync + 'static;

    /// Create the initial state.
    fn create_initial_state(&self) -> Self::State;
}

/// Helper to implement `initial_state` for any `StatefulPlugin`.
///
/// Call this inside your `Plugin::initial_state()` override:
/// ```ignore
/// fn initial_state(&self) -> Option<Arc<dyn Any + Send + Sync>> {
///     Some(stateful_initial_state(self))
/// }
/// ```
pub fn stateful_initial_state<P: StatefulPlugin>(plugin: &P) -> Arc<dyn Any + Send + Sync> {
    let state = parking_lot::RwLock::new(plugin.create_initial_state());
    Arc::new(state) as Arc<dyn Any + Send + Sync>
}

// ---- Global registry ----

static REGISTRY: std::sync::LazyLock<RwLock<Vec<Arc<dyn Plugin>>>> =
    std::sync::LazyLock::new(|| RwLock::new(Vec::new()));

/// Register a plugin globally.
pub fn register_plugin(plugin: impl Plugin) {
    let mut plugins = REGISTRY.write();
    plugins.push(Arc::new(plugin));
}

/// Get all registered plugins.
pub fn registered_plugins() -> Vec<Arc<dyn Plugin>> {
    let plugins = REGISTRY.read();
    plugins.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fish_core::rule::Rule;

    struct TestPlugin {
        meta: PluginMetadata,
        handlers: Vec<MessageHandler>,
    }

    impl TestPlugin {
        fn new() -> Self {
            Self {
                meta: PluginMetadata {
                    id: "test".into(),
                    name: "测试插件".into(),
                    description: "测试".into(),
                    ..Default::default()
                },
                handlers: vec![MessageHandler::new("handler1", RouteHint::Fallback, None, Arc::new(|_: HandlerContext| {
                    Box::pin(async { Ok(()) })
                }))],
            }
        }
    }

    impl Plugin for TestPlugin {
        fn metadata(&self) -> &PluginMetadata {
            &self.meta
        }

        fn message_handlers(&self) -> &[MessageHandler] {
            &self.handlers
        }
    }

    #[test]
    fn t2_1_metadata_defaults() {
        let meta = PluginMetadata::default();
        assert_eq!(meta.id, "");
        assert_eq!(meta.name, "");
        assert_eq!(meta.version, "1.0.0");
        assert_eq!(meta.author, "Unknown");
    }

    #[test]
    fn t2_2_register_and_list() {
        register_plugin(TestPlugin::new());
        let plugins = registered_plugins();
        let found = plugins.iter().any(|p| p.metadata().id == "test");
        assert!(found);
    }

    #[test]
    fn t2_4_message_handler_construct() {
        let handler = MessageHandler::new("h1", RouteHint::Fallback, None, Arc::new(|_: HandlerContext| {
            Box::pin(async { Ok(()) })
        }));
        assert!(handler.rule.is_none());
    }

    #[test]
    fn t2_3_duplicate_registration_allowed() {
        struct DupPlugin {
            meta: PluginMetadata,
        }
        impl Plugin for DupPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
        }
        register_plugin(DupPlugin { meta: PluginMetadata { id: "dup".into(), name: "".into(), description: "".into(), ..Default::default() } });
        register_plugin(DupPlugin { meta: PluginMetadata { id: "dup".into(), name: "".into(), description: "".into(), ..Default::default() } });
        let plugins = registered_plugins();
        let count = plugins.iter().filter(|p| p.metadata().id == "dup").count();
        assert_eq!(count, 2, "duplicate registration should be allowed at registry level");
    }

    #[test]
    fn t2_18_default_event_handlers() -> anyhow::Result<()> {
        struct EmptyPlugin {
            meta: PluginMetadata,
        }
        impl Plugin for EmptyPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
        }

        let plugin = EmptyPlugin { meta: PluginMetadata::default() };
        let handlers = plugin.event_handlers();
        assert!(handlers.is_empty());
        Ok(())
    }

    #[test]
    fn t2_19_default_message_handlers() -> anyhow::Result<()> {
        struct EmptyPlugin {
            meta: PluginMetadata,
        }
        impl Plugin for EmptyPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
        }

        let plugin = EmptyPlugin { meta: PluginMetadata::default() };
        let handlers = plugin.message_handlers();
        assert!(handlers.is_empty());
        Ok(())
    }

    #[test]
    fn t2_20_event_handler_construct() -> anyhow::Result<()> {
        let handler = EventHandler::new("test_event", Arc::new(|_, _, _, _| Box::pin(async { Ok(()) })));
        assert_eq!(handler.id, "test_event");
        assert!(handler.rule.is_none());
        Ok(())
    }

    #[test]
    fn t2_32_plugin_with_event_handlers() -> anyhow::Result<()> {
        struct EventPlugin {
            meta: PluginMetadata,
        }
        impl Plugin for EventPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn event_handlers(&self) -> HashMap<String, Vec<EventHandler>> {
                let mut map = HashMap::new();
                map.insert("notice".into(), vec![EventHandler::new("notice_handler", Arc::new(|_, _, _, _| Box::pin(async { Ok(()) })))]);
                map
            }
        }

        let plugin = EventPlugin { meta: PluginMetadata { id: "event_test".into(), ..Default::default() } };
        let handlers = plugin.event_handlers();
        assert_eq!(handlers.len(), 1);
        assert!(handlers.contains_key("notice"));
        Ok(())
    }

    #[test]
    fn t2_33_plugin_metadata_full() -> anyhow::Result<()> {
        let meta = PluginMetadata {
            id: "custom".into(),
            name: "Custom".into(),
            description: "desc".into(),
            version: "2.0.0".into(),
            author: "tester".into(),
        };
        assert_eq!(meta.id, "custom");
        assert_eq!(meta.name, "Custom");
        assert_eq!(meta.description, "desc");
        assert_eq!(meta.version, "2.0.0");
        assert_eq!(meta.author, "tester");
        Ok(())
    }

    #[test]
    fn t2_34_register_plugin_increases_registry() -> anyhow::Result<()> {
        struct RegPlugin {
            meta: PluginMetadata,
        }
        impl Plugin for RegPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
        }

        let before = registered_plugins().len();
        register_plugin(RegPlugin { meta: PluginMetadata { id: "reg_check".into(), ..Default::default() } });
        let after = registered_plugins().len();
        assert!(after >= before + 1, "registry should have grown");
        Ok(())
    }

    #[test]
    fn t2_35_plugin_metadata_clone() -> anyhow::Result<()> {
        let meta = PluginMetadata {
            id: "clone_test".into(),
            name: "Clone".into(),
            description: "desc".into(),
            version: "3.0".into(),
            author: "author".into(),
        };
        let cloned = meta.clone();
        assert_eq!(cloned.id, meta.id);
        assert_eq!(cloned.name, meta.name);
        assert_eq!(cloned.version, meta.version);
        Ok(())
    }

    #[test]
    fn t2_36_plugin_metadata_debug() -> anyhow::Result<()> {
        let meta = PluginMetadata::default();
        let debug_str = format!("{:?}", meta);
        assert!(debug_str.contains("PluginMetadata"), "debug should contain struct name");
        Ok(())
    }

    #[test]
    fn t2_37_message_handler_without_rule() -> anyhow::Result<()> {
        let handler = MessageHandler::new("h1", RouteHint::Fallback, None, Arc::new(|_: HandlerContext| {
            Box::pin(async { Ok(()) })
        }));
        assert!(handler.rule.is_none());
        Ok(())
    }
}
