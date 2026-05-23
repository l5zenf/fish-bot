use std::sync::Arc;
use crate::adapter::BaseAdapter;
use crate::model::MessageEvent;
use once_cell::sync::Lazy;
use std::sync::Mutex;

pub mod echo;

pub trait Plugin: Send + Sync + 'static {
    fn on_message(&self, event: &MessageEvent, adapter: Arc<dyn BaseAdapter>);
}

type PluginCallback = Box<dyn Fn(&MessageEvent, Arc<dyn BaseAdapter>) + Send + Sync>;

static REGISTRY: Lazy<Mutex<Vec<Box<dyn Plugin>>>> = Lazy::new(|| Mutex::new(Vec::new()));

pub fn register_plugin<P: Plugin + 'static>(plugin: P) {
    let mut plugins = REGISTRY.lock().unwrap();
    plugins.push(Box::new(plugin));
}

pub fn dispatch_event(event: &MessageEvent, adapter: Arc<dyn BaseAdapter>) {
    let plugins = REGISTRY.lock().unwrap();
    for plugin in plugins.iter() {
        plugin.on_message(event, adapter.clone());
    }
}
