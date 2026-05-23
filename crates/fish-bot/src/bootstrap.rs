/// Application bootstrap — tracing, panic hooks, etc.
///
/// Respects `RUST_LOG` env var first; falls back to `default_level`.
/// To silence noisy deps: `RUST_LOG=info,reqwest=warn,tungstenite=warn`.
pub fn init(default_level: &str) {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .init();
}
