use std::collections::HashMap;
use std::sync::Arc;

use crate::plugin::Plugin;

/// Plugin manager.
pub struct PluginManager {
    pub plugins: HashMap<String, Arc<dyn Plugin>>,
}

impl PluginManager {
    pub fn new() -> Self {
        Self {
            plugins: HashMap::new(),
        }
    }

    /// Load all plugins — discovers globally registered plugins.
    pub fn load_all_plugins(&mut self) {
        let registered = crate::plugin::registered_plugins();
        for plugin in registered {
            let meta = plugin.metadata();
            let name = meta.id.clone();
            if self.plugins.contains_key(&name) {
                tracing::warn!("Plugin naming conflict, skipping: [{}]", name);
                continue;
            }
            self.plugins.insert(name.clone(), plugin.clone());
            tracing::info!(
                "Successfully loaded plugin: [{}] v{}",
                name,
                meta.version
            );
        }
    }

    /// Number of loaded plugins.
    pub fn len(&self) -> usize {
        self.plugins.len()
    }

    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }
}

impl Default for PluginManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::{register_plugin, Plugin, PluginMetadata, MessageHandler};

    struct TestPluginA;
    impl Plugin for TestPluginA {
        fn metadata(&self) -> PluginMetadata {
            PluginMetadata { id: "plugin_a".into(), name: "A".into(), description: "".into(), ..Default::default() }
        }
        fn message_handlers(&self) -> Vec<MessageHandler> { vec![] }
    }

    struct TestPluginB;
    impl Plugin for TestPluginB {
        fn metadata(&self) -> PluginMetadata {
            PluginMetadata { id: "plugin_b".into(), name: "B".into(), description: "".into(), ..Default::default() }
        }
        fn message_handlers(&self) -> Vec<MessageHandler> { vec![] }
    }

    #[test]
    fn t2_14_new_empty_manager() {
        let mgr = PluginManager::new();
        assert_eq!(mgr.len(), 0);
        assert!(mgr.is_empty());
    }

    #[test]
    fn t2_15_load_all_plugins() {
        register_plugin(TestPluginA);
        register_plugin(TestPluginB);

        let mut mgr = PluginManager::new();
        mgr.load_all_plugins();
        // Can't assert exact len (shared global registry with other tests)
        assert!(mgr.plugins.contains_key("plugin_a"), "plugin_a should be loaded");
        assert!(mgr.plugins.contains_key("plugin_b"), "plugin_b should be loaded");
    }

    #[test]
    fn t2_17_is_empty_before_after() {
        let mgr = PluginManager::new();
        assert!(mgr.is_empty());
    }

    #[test]
    fn t2_16_duplicate_id_skipped() {
        struct PluginX;
        impl Plugin for PluginX {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata { id: "test_dupe_id".into(), name: "X".into(), description: "".into(), ..Default::default() }
            }
            fn message_handlers(&self) -> Vec<MessageHandler> { vec![] }
        }
        struct PluginXDuplicate;
        impl Plugin for PluginXDuplicate {
            fn metadata(&self) -> PluginMetadata {
                PluginMetadata { id: "test_dupe_id".into(), name: "X2".into(), description: "".into(), ..Default::default() }
            }
            fn message_handlers(&self) -> Vec<MessageHandler> { vec![] }
        }

        register_plugin(PluginX);
        register_plugin(PluginXDuplicate);

        let mut mgr = PluginManager::new();
        mgr.load_all_plugins();
        // Only one should be loaded for "test_dupe_id" (first one = "X" wins)
        assert!(mgr.plugins.contains_key("test_dupe_id"), "plugin should be loaded");
        // The first registered plugin's name wins
        assert_eq!(mgr.plugins["test_dupe_id"].metadata().name, "X",
            "first registered plugin should be the one loaded");
    }
}
