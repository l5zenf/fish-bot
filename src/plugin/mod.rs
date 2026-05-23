use std::sync::Arc;
use crate::adapter::BaseAdapter;
use crate::model::MessageEvent;
use once_cell::sync::Lazy;
use std::sync::Mutex;
use async_trait::async_trait;

pub mod echo;

#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    async fn on_message(&self, event: &MessageEvent, adapter: Arc<dyn BaseAdapter>);
}

static REGISTRY: Lazy<Mutex<Vec<Box<dyn Plugin>>>> = Lazy::new(|| Mutex::new(Vec::new()));

pub fn register_plugin<P: Plugin + 'static>(plugin: P) {
    let mut plugins = REGISTRY.lock().unwrap();
    plugins.push(Box::new(plugin));
}

pub async fn dispatch_event(event: &MessageEvent, adapter: Arc<dyn BaseAdapter>) {
    let plugins = REGISTRY.lock().unwrap();
    for plugin in plugins.iter() {
        plugin.on_message(event, adapter.clone()).await;
    }
}
