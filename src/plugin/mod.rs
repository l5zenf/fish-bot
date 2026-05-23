use std::sync::Arc;
use crate::adapter::BaseAdapter;
use crate::model::MessageEvent;
use once_cell::sync::Lazy;
use async_trait::async_trait;

pub mod echo;

#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    async fn on_message(&self, event: &MessageEvent, adapter: Arc<dyn BaseAdapter>);
}

static REGISTRY: Lazy<std::sync::Mutex<Vec<Arc<dyn Plugin>>>> =
    Lazy::new(|| std::sync::Mutex::new(Vec::new()));

pub fn register_plugin<P: Plugin + 'static>(plugin: P) {
    let mut plugins = REGISTRY.lock().unwrap();
    plugins.push(Arc::new(plugin));
}

pub async fn dispatch_event(event: &MessageEvent, adapter: Arc<dyn BaseAdapter>) {
    let plugins: Vec<Arc<dyn Plugin>> = {
        let guard = REGISTRY.lock().unwrap();
        guard.clone()
    };
    for plugin in &plugins {
        plugin.on_message(event, adapter.clone()).await;
    }
}
