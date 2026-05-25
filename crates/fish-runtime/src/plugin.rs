use std::any::Any;
use std::sync::Arc;

use crate::handlers::{EventHandler, MessageHandler};
use crate::runtime::RuntimeConfig;

pub type PluginState = Arc<dyn Any + Send + Sync>;

#[derive(Debug, Clone)]
pub struct PluginMetadata {
    pub id: String,
    pub name: String,
}

impl Default for PluginMetadata {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
        }
    }
}

pub trait Plugin: Send + Sync + 'static {
    fn metadata(&self) -> &PluginMetadata;

    #[doc(hidden)]
    fn message_handlers(&self) -> &[MessageHandler] {
        &[]
    }

    #[doc(hidden)]
    fn event_handlers(&self) -> &[EventHandler] {
        &[]
    }

    #[doc(hidden)]
    fn runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig::default()
    }

    #[doc(hidden)]
    fn initial_state(&self) -> Option<PluginState> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t2_1_metadata_defaults() {
        let meta = PluginMetadata::default();
        assert_eq!(meta.id, "");
        assert_eq!(meta.name, "");
    }

    #[test]
    fn t2_18_default_event_handlers() -> anyhow::Result<()> {
        struct EmptyPlugin {
            meta: PluginMetadata,
        }
        impl Plugin for EmptyPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
        }

        let plugin = EmptyPlugin {
            meta: PluginMetadata::default(),
        };
        assert!(plugin.event_handlers().is_empty());
        Ok(())
    }

    #[test]
    fn t2_19_default_message_handlers() -> anyhow::Result<()> {
        struct EmptyPlugin {
            meta: PluginMetadata,
        }
        impl Plugin for EmptyPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
        }

        let plugin = EmptyPlugin {
            meta: PluginMetadata::default(),
        };
        assert!(plugin.message_handlers().is_empty());
        Ok(())
    }

    #[test]
    fn t2_33_plugin_metadata_full() -> anyhow::Result<()> {
        let meta = PluginMetadata {
            id: "custom".into(),
            name: "Custom".into(),
        };
        assert_eq!(meta.id, "custom");
        assert_eq!(meta.name, "Custom");
        Ok(())
    }

    #[test]
    fn t2_34_initial_state_wraps_rwlock() -> anyhow::Result<()> {
        struct StatefulTestPlugin;
        impl Plugin for StatefulTestPlugin {
            fn metadata(&self) -> &PluginMetadata {
                static META: std::sync::LazyLock<PluginMetadata> =
                    std::sync::LazyLock::new(PluginMetadata::default);
                &META
            }
            fn initial_state(&self) -> Option<PluginState> {
                Some(Arc::new(tokio::sync::RwLock::new(7usize)))
            }
        }

        let state = StatefulTestPlugin
            .initial_state()
            .expect("state should exist");
        let lock = state
            .clone()
            .downcast::<tokio::sync::RwLock<usize>>()
            .expect("state should be RwLock<usize>");
        let rt = tokio::runtime::Runtime::new()?;
        let value = rt.block_on(async { *lock.read().await });
        assert_eq!(value, 7);
        Ok(())
    }

    #[test]
    fn t2_35_plugin_metadata_clone() -> anyhow::Result<()> {
        let meta = PluginMetadata {
            id: "clone_test".into(),
            name: "Clone".into(),
        };
        let cloned = meta.clone();
        assert_eq!(cloned.id, meta.id);
        assert_eq!(cloned.name, meta.name);
        Ok(())
    }

    #[test]
    fn t2_36_plugin_metadata_debug() -> anyhow::Result<()> {
        let meta = PluginMetadata::default();
        let debug_str = format!("{:?}", meta);
        assert!(debug_str.contains("PluginMetadata"));
        Ok(())
    }
}
