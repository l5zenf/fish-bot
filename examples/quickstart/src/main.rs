mod app;

use fish_core::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    app::run().await
}
