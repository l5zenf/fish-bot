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

    #[test]
    fn t2_18_default_event_handlers() -> anyhow::Result<()> {
        struct EmptyPlugin;
        impl Plugin for EmptyPlugin {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata::default()
            }
        }

        let plugin = EmptyPlugin;
        let handlers = plugin.event_handlers();
        assert!(handlers.is_empty());
        Ok(())
    }

    #[test]
    fn t2_19_default_message_handlers() -> anyhow::Result<()> {
        struct EmptyPlugin;
        impl Plugin for EmptyPlugin {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata::default()
            }
        }

        let plugin = EmptyPlugin;
        let handlers = plugin.message_handlers();
        assert!(handlers.is_empty());
        Ok(())
    }

    #[test]
    fn t2_20_event_handler_construct() -> anyhow::Result<()> {
        let handler_with_rule = EventHandler {
            func: Arc::new(|_, _, _| Box::pin(async {})),
            rule: Some(Rule::new(|_| true)),
        };
        assert!(handler_with_rule.rule.is_some());

        let handler_no_rule = EventHandler {
            func: Arc::new(|_, _, _| Box::pin(async {})),
            rule: None,
        };
        assert!(handler_no_rule.rule.is_none());
        Ok(())
    }

    #[test]
    fn t2_32_plugin_with_event_handlers() -> anyhow::Result<()> {
        struct EventPlugin;
        impl Plugin for EventPlugin {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata { id: "event_test".into(), name: "".into(), description: "".into(), ..Default::default() }
            }
            fn event_handlers(&self) -> HashMap<String, Vec<EventHandler>> {
                let mut map = HashMap::new();
                map.insert("notice".into(), vec![EventHandler {
                    func: Arc::new(|_, _, _| Box::pin(async {})),
                    rule: None,
                }]);
                map
            }
        }

        let plugin = EventPlugin;
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
        struct RegPlugin;
        impl Plugin for RegPlugin {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata { id: "reg_check".into(), name: "".into(), description: "".into(), ..Default::default() }
            }
        }

        let before = registered_plugins().len();
        register_plugin(RegPlugin);
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
        let handler = MessageHandler {
            func: Arc::new(|_, _, _| Box::pin(async {})),
            rule: None,
        };
        assert!(handler.rule.is_none());
        Ok(())
    }
}
