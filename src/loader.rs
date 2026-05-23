use std::collections::HashMap;
use std::sync::Arc;

use crate::plugin::Plugin;

/// Plugin manager matching Python loader.py PluginManager.
pub struct PluginManager {
    pub plugins: HashMap<String, Arc<dyn Plugin>>,
}

impl PluginManager {
    pub fn new() -> Self {
        Self {
            plugins: HashMap::new(),
        }
    }

    /// Load all plugins — in Rust this discovers globally registered plugins.
    /// Matches Python PluginManager.load_all_plugins().
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
