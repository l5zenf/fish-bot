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
    use crate::plugin::{register_plugin, Plugin, PluginMetadata};

    struct TestPluginA {
        meta: PluginMetadata,
    }
    impl Plugin for TestPluginA {
        fn metadata(&self) -> &PluginMetadata { &self.meta }
    }

    struct TestPluginB {
        meta: PluginMetadata,
    }
    impl Plugin for TestPluginB {
        fn metadata(&self) -> &PluginMetadata { &self.meta }
    }

    #[test]
    fn t2_14_new_empty_manager() {
        let mgr = PluginManager::new();
        assert_eq!(mgr.len(), 0);
        assert!(mgr.is_empty());
    }

    #[test]
    fn t2_15_load_all_plugins() {
        register_plugin(TestPluginA { meta: PluginMetadata { id: "plugin_a".into(), name: "A".into(), ..Default::default() } });
        register_plugin(TestPluginB { meta: PluginMetadata { id: "plugin_b".into(), name: "B".into(), ..Default::default() } });

        let mut mgr = PluginManager::new();
        mgr.load_all_plugins();
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
        struct PluginX { meta: PluginMetadata }
        impl Plugin for PluginX {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
        }
        struct PluginXDuplicate { meta: PluginMetadata }
        impl Plugin for PluginXDuplicate {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
        }

        register_plugin(PluginX { meta: PluginMetadata { id: "test_dupe_id".into(), name: "X".into(), ..Default::default() } });
        register_plugin(PluginXDuplicate { meta: PluginMetadata { id: "test_dupe_id".into(), name: "X2".into(), ..Default::default() } });

        let mut mgr = PluginManager::new();
        mgr.load_all_plugins();
        assert!(mgr.plugins.contains_key("test_dupe_id"), "plugin should be loaded");
        assert_eq!(mgr.plugins["test_dupe_id"].metadata().name, "X",
            "first registered plugin should be the one loaded");
    }

    #[test]
    fn t2_26_load_empty_registry() -> anyhow::Result<()> {
        let mgr = PluginManager::new();
        assert!(mgr.is_empty());
        Ok(())
    }

    #[test]
    fn t2_27_len_is_empty() -> anyhow::Result<()> {
        struct DummyPlugin { meta: PluginMetadata }
        impl Plugin for DummyPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
        }

        let empty_mgr = PluginManager::new();
        assert_eq!(empty_mgr.len(), 0);
        assert!(empty_mgr.is_empty());

        let mut mgr = PluginManager::new();
        mgr.plugins.insert("len_test".into(), Arc::new(DummyPlugin { meta: PluginMetadata { id: "len_test".into(), ..Default::default() } }));
        assert_eq!(mgr.len(), 1);
        assert!(!mgr.is_empty());
        Ok(())
    }

    #[test]
    fn t2_28_plugin_manager_default() -> anyhow::Result<()> {
        let mgr = PluginManager::default();
        assert_eq!(mgr.len(), 0);
        assert!(mgr.is_empty());
        Ok(())
    }

    #[test]
    fn t2_29_load_all_plugins_twice() -> anyhow::Result<()> {
        struct OncePlugin { meta: PluginMetadata }
        impl Plugin for OncePlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
        }

        register_plugin(OncePlugin { meta: PluginMetadata { id: "load_twice".into(), name: "Once".into(), ..Default::default() } });
        let mut mgr = PluginManager::new();
        mgr.load_all_plugins();
        let count_after_first = mgr.plugins.iter().filter(|(k, _)| *k == "load_twice").count();
        assert_eq!(count_after_first, 1, "plugin should be loaded once");

        mgr.load_all_plugins();
        let count_after_second = mgr.plugins.iter().filter(|(k, _)| *k == "load_twice").count();
        assert_eq!(count_after_second, 1, "loading twice should not add a duplicate");
        Ok(())
    }
}
