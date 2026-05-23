use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_core::event::MessageEvent;
use fish_core::rule::Rule;

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

/// A message handler registered by a plugin.
pub struct MessageHandler {
    pub func: Arc<
        dyn Fn(MessageEvent, Arc<dyn BaseAdapter>, Arc<Ctx>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            + Send
            + Sync,
    >,
    pub rule: Option<Rule>,
}

/// An event handler registered by a plugin.
/// Handles non-message events (notices, requests, meta events).
pub struct EventHandler {
    pub func: Arc<
        dyn Fn(serde_json::Value, Arc<dyn BaseAdapter>, Arc<Ctx>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            + Send
            + Sync,
    >,
    pub rule: Option<Rule>,
}

/// Plugin trait.
pub trait Plugin: Send + Sync + 'static {
    fn metadata(&self) -> PluginMetadata;

    /// Message handlers — each handler has a func and an optional Rule.
    fn message_handlers(&self) -> Vec<MessageHandler> {
        Vec::new()
    }

    /// Event handlers keyed by event type (e.g. "notice", "request", "meta_event").
    fn event_handlers(&self) -> HashMap<String, Vec<EventHandler>> {
        HashMap::new()
    }
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

    struct TestPlugin;

    impl Plugin for TestPlugin {
        fn metadata(&self) -> PluginMetadata {
            PluginMetadata {
                id: "test".into(),
                name: "测试插件".into(),
                description: "测试".into(),
                ..Default::default()
            }
        }

        fn message_handlers(&self) -> Vec<MessageHandler> {
            vec![MessageHandler {
                func: Arc::new(|_, _, _| Box::pin(async {})),
                rule: None,
            }]
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
        register_plugin(TestPlugin);
        let plugins = registered_plugins();
        // We can't assert exact length because other tests may register too,
        // but we can check our plugin is present
        let found = plugins.iter().any(|p| p.metadata().id == "test");
        assert!(found);
    }

    #[test]
    fn t2_4_message_handler_construct() {
        let handler = MessageHandler {
            func: Arc::new(|_, _, _| Box::pin(async {})),
            rule: None,
        };
        assert!(handler.rule.is_none());
    }

    #[test]
    fn t2_3_duplicate_registration_allowed() {
        struct DupPlugin;
        impl Plugin for DupPlugin {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata { id: "dup".into(), name: "".into(), description: "".into(), ..Default::default() }
            }
            fn message_handlers(&self) -> Vec<MessageHandler> { vec![] }
        }
        // Registering the same plugin type twice should not panic
        register_plugin(DupPlugin);
        register_plugin(DupPlugin);
        let plugins = registered_plugins();
        let count = plugins.iter().filter(|p| p.metadata().id == "dup").count();
        assert_eq!(count, 2, "duplicate registration should be allowed at registry level");
    }
}
