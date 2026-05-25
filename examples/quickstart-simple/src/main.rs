mod app;

use clap::Parser;
use fish_rt_adapter::Result;
use fish_rt_adapter::import_browser_cookies;

#[derive(Debug, Parser)]
struct Args {
    /// Raw browser Cookie header. The runtime will persist it to fish_auth.json before startup.
    #[arg(long)]
    cookies: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(raw) = args.cookies.as_deref() {
        let report = import_browser_cookies(raw).await?;
        println!(
            "imported {} cookies into {}",
            report.imported,
            report.path.display()
        );
    }

    app::run().await
}
