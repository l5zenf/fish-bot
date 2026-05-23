mod error;
mod model;
mod protocol;
mod adapter;
mod plugin;

use adapter::BaseAdapter;
use adapter::fish::FishWebSocketAdapter;
use plugin::echo::EchoPlugin;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let adapter = Arc::new(FishWebSocketAdapter::new()) as Arc<dyn BaseAdapter>;

    plugin::register_plugin(EchoPlugin);
    tracing::info!("FishBot starting...");

    adapter.run().await?;

    Ok(())
}
