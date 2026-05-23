use std::collections::HashMap;
use std::sync::Arc;

use crate::adapter::BaseAdapter;
use crate::ctx::Ctx;
use crate::event::MessageEvent;
use crate::rule::Rule;

pub mod actor;
pub mod echo;

/// Plugin metadata, matching Python plugin.py PluginMetadata.
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

/// A message handler registered by a plugin, matching Python's message handler dict.
pub struct MessageHandler {
    pub func: Arc<
        dyn Fn(MessageEvent, Arc<dyn BaseAdapter>, Arc<Ctx>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            + Send
            + Sync,
    >,
    pub rule: Option<Rule>,
}

/// An event handler registered by a plugin, matching Python's event handler dict.
/// Handles non-message events (notices, requests, meta events).
pub struct EventHandler {
    pub func: Arc<
        dyn Fn(serde_json::Value, Arc<dyn BaseAdapter>, Arc<Ctx>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            + Send
            + Sync,
    >,
    pub rule: Option<Rule>,
}

/// Plugin trait, matching Python plugin.py Plugin class.
pub trait Plugin: Send + Sync + 'static {
    fn metadata(&self) -> PluginMetadata;

    /// Message handlers — each handler has a func and an optional Rule.
    fn message_handlers(&self) -> Vec<MessageHandler> {
        Vec::new()
    }

    /// Event handlers keyed by event type (e.g. "notice", "request", "meta_event").
    /// Matches Python Plugin.event_handlers: dict[str, list[dict]].
    fn event_handlers(&self) -> HashMap<String, Vec<EventHandler>> {
        HashMap::new()
    }
}

// ---- Global registry, matching Python PluginManager pattern ----

static REGISTRY: std::sync::LazyLock<std::sync::RwLock<Vec<Arc<dyn Plugin>>>> =
    std::sync::LazyLock::new(|| std::sync::RwLock::new(Vec::new()));

/// Register a plugin globally.
pub fn register_plugin(plugin: impl Plugin) {
    let mut plugins = REGISTRY.write().unwrap();
    plugins.push(Arc::new(plugin));
}

/// Get all registered plugins.
pub fn registered_plugins() -> Vec<Arc<dyn Plugin>> {
    let plugins = REGISTRY.read().unwrap();
    plugins.clone()
}
