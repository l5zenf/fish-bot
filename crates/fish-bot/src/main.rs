use std::sync::Arc;

use kameo::prelude::*;

use fish_adapter::adapter::BaseAdapter;
use fish_adapter::fish::FishWebSocketAdapter;
use fish_core::ctx::Ctx;
mod bootstrap;
use fish_plugin::loader::PluginManager;
use fish_plugin::plugin::actor::PluginActor;
use fish_plugin::plugin::echo::EchoPlugin;
use fish_plugin::plugin::register_plugin;

mod bot;
use bot::{Bot, DispatchEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    bootstrap::init("DEBUG");

    // ---- Build shared dependency container ----
    let ctx = Arc::new(Ctx::new());

    // Register plugins
    register_plugin(EchoPlugin::new());

    // Load plugins into PluginManager
    let mut plugin_manager = PluginManager::new();
    plugin_manager.load_all_plugins();

    // Spawn each plugin as an isolated kameo actor
    let mut plugin_refs: Vec<(ActorRef<PluginActor>, Arc<dyn fish_plugin::plugin::Plugin>)> = Vec::new();
    let plugin_count = plugin_manager.plugins.len();
    for (id, plugin) in &plugin_manager.plugins {
        let actor_ref = PluginActor::spawn(PluginActor::new(Arc::clone(plugin)));
        tracing::info!("PluginActor spawned: [{}]", id);
        plugin_refs.push((actor_ref, Arc::clone(plugin)));
    }

    // Create adapter (keep concrete Arc<FishWebSocketAdapter> for run_arc)
    let adapter = Arc::new(FishWebSocketAdapter::new());

    // Create a trait-object clone for the Bot actor
    let adapter_dyn: Arc<dyn BaseAdapter> = Arc::clone(&adapter) as Arc<dyn BaseAdapter>;

    // Spawn Bot actor — fans out events to all PluginActors with shared Ctx
    let bot_ref = Bot::spawn(Bot::new(
        Arc::clone(&adapter_dyn),
        plugin_refs,
        Arc::clone(&ctx),
    ));
    tracing::info!("Bot started, loaded {} plugin(s)", plugin_count);

    // Wire adapter callback to forward events into the Bot actor
    let bot = bot_ref.clone();
    adapter_dyn.set_callback(Box::new(move |event| {
        let b = bot.clone();
        tokio::spawn(async move {
            let _ = b.tell(DispatchEvent { event }).await;
        });
    }));

    // Run adapter (blocks until shutdown)
    tracing::info!("Starting communication adapter...");
    adapter.run_arc().await?;

    Ok(())
}
