/// Initialize the logging system, matching Python logger.py init_logger().
pub fn init_logger(level: &str) {
    use tracing_subscriber::EnvFilter;

    let filter = level
        .parse::<EnvFilter>()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}
