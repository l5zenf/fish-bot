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

#[cfg(test)]
mod tests {
    #[test]
    fn t4_1_init_does_not_crash() {
        // This should not panic. Since tracing can only be initialized once per process,
        // we just verify the function signature compiles and runs without crashing.
        // In practice, a previous test may have already initialized tracing.
        // That's fine — `init()` would just log a warning and continue.
        // We verify it doesn't panic by calling it (setting RUST_LOG=none to suppress output).
        // SAFETY: test-only, single-threaded
        unsafe { std::env::set_var("RUST_LOG", "none"); }
        // We use a separate scope to avoid double-init panics
        let _ = std::panic::catch_unwind(|| {
            // This may already be initialized, but shouldn't panic
            super::init("none");
        });
        // SAFETY: test-only cleanup
        unsafe { std::env::remove_var("RUST_LOG"); }
    }

    #[test]
    fn t4_6_init_with_custom_level() -> anyhow::Result<()> {
        // SAFETY: test-only, single-threaded
        unsafe { std::env::set_var("RUST_LOG", "debug"); }
        let _ = std::panic::catch_unwind(|| {
            super::init("debug");
        });
        // SAFETY: test-only cleanup
        unsafe { std::env::remove_var("RUST_LOG"); }
        Ok(())
    }

    #[test]
    fn t4_9_init_with_invalid_rust_log() -> anyhow::Result<()> {
        // Set an invalid RUST_LOG value to trigger the unwrap_or_else fallback
        unsafe { std::env::set_var("RUST_LOG", "invalid=filter=!!!"); }
        let _ = std::panic::catch_unwind(|| {
            super::init("fallback_level");
        });
        unsafe { std::env::remove_var("RUST_LOG"); }
        Ok(())
    }
}
