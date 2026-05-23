use std::sync::Arc;

use kameo::prelude::*;

use fish_bot::adapter::BaseAdapter;
use fish_bot::adapter::fish::FishWebSocketAdapter;
use fish_bot::bot::{Bot, DispatchEvent};
use fish_bot::ctx::Ctx;
use fish_bot::loader::PluginManager;
use fish_bot::logger;
use fish_bot::plugin::actor::PluginActor;
use fish_bot::plugin::echo::EchoPlugin;
use fish_bot::plugin::register_plugin;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logger::init_logger("DEBUG");

    // ---- Build shared dependency container ----
    let ctx = Arc::new(Ctx::new());
    // ctx.insert(MyDbPool::new(...));      // 示例：以后可以在这里注入 DB
    // ctx.insert(MyConfig::load(...));     // 示例：以后可以在这里注入 Config

    // Register plugins
    register_plugin(EchoPlugin::new());

    // Load plugins into PluginManager
    let mut plugin_manager = PluginManager::new();
    plugin_manager.load_all_plugins();

    // Spawn each plugin as an isolated kameo actor
    let mut plugin_refs: Vec<ActorRef<PluginActor>> = Vec::new();
    let plugin_count = plugin_manager.plugins.len();
    for (id, plugin) in &plugin_manager.plugins {
        let actor_ref = PluginActor::spawn(PluginActor::new(Arc::clone(plugin)));
        tracing::info!("PluginActor spawned: [{}]", id);
        plugin_refs.push(actor_ref);
    }

    // Create adapter
    let adapter = Arc::new(FishWebSocketAdapter::new()) as Arc<dyn BaseAdapter>;

    // Spawn Bot actor — fans out events to all PluginActors with shared Ctx
    let bot_ref = Bot::spawn(Bot::new(
        Arc::clone(&adapter),
        plugin_refs,
        Arc::clone(&ctx),
    ));
    tracing::info!("Bot started, loaded {} plugin(s)", plugin_count);

    // Wire adapter callback to forward events into the Bot actor
    let bot = bot_ref.clone();
    adapter.set_callback(Box::new(move |event| {
        let b = bot.clone();
        tokio::spawn(async move {
            let _ = b.tell(DispatchEvent { event }).await;
        });
    }));

    // Run adapter (blocks until shutdown)
    tracing::info!("Starting communication adapter...");
    adapter.run().await?;

    Ok(())
}
