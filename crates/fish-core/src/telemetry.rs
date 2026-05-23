use std::sync::atomic::{AtomicUsize, Ordering};

/// Lock-free metrics counters shared across Bot, PluginActor, and handlers.
#[derive(Debug)]
pub struct Telemetry {
    // ---- Bot routing layer ----
    pub messages_received: AtomicUsize,
    pub exact_route_hits: AtomicUsize,
    pub prefix_route_hits: AtomicUsize,
    pub keyword_route_hits: AtomicUsize,
    pub fallback_dispatches: AtomicUsize,
    pub handler_dispatches: AtomicUsize,
    pub reply_failures: AtomicUsize,
    pub unmatched_messages: AtomicUsize,

    // ---- PluginActor handler layer ----
    pub handler_started: AtomicUsize,
    pub handler_succeeded: AtomicUsize,
    pub handler_failed: AtomicUsize,
    pub handler_timed_out: AtomicUsize,

    // ---- Queue strategy ----
    pub drop_newest_drops: AtomicUsize,
    pub drop_oldest_enqueues: AtomicUsize,
    pub drop_oldest_oldest_discards: AtomicUsize,
    pub queued_handler_succeeded: AtomicUsize,
    pub queued_handler_failed: AtomicUsize,
    pub queued_handler_timed_out: AtomicUsize,
}

/// A point-in-time copy of all Telemetry counters.
#[derive(Debug, Clone, Default)]
pub struct TelemetrySnapshot {
    pub messages_received: usize,
    pub exact_route_hits: usize,
    pub prefix_route_hits: usize,
    pub keyword_route_hits: usize,
    pub fallback_dispatches: usize,
    pub handler_dispatches: usize,
    pub reply_failures: usize,
    pub unmatched_messages: usize,
    pub handler_started: usize,
    pub handler_succeeded: usize,
    pub handler_failed: usize,
    pub handler_timed_out: usize,
    pub drop_newest_drops: usize,
    pub drop_oldest_enqueues: usize,
    pub drop_oldest_oldest_discards: usize,
    pub queued_handler_succeeded: usize,
    pub queued_handler_failed: usize,
    pub queued_handler_timed_out: usize,
}

impl Telemetry {
    pub fn new() -> Self {
        Self {
            messages_received: AtomicUsize::new(0),
            exact_route_hits: AtomicUsize::new(0),
            prefix_route_hits: AtomicUsize::new(0),
            keyword_route_hits: AtomicUsize::new(0),
            fallback_dispatches: AtomicUsize::new(0),
            handler_dispatches: AtomicUsize::new(0),
            reply_failures: AtomicUsize::new(0),
            unmatched_messages: AtomicUsize::new(0),
            handler_started: AtomicUsize::new(0),
            handler_succeeded: AtomicUsize::new(0),
            handler_failed: AtomicUsize::new(0),
            handler_timed_out: AtomicUsize::new(0),
            drop_newest_drops: AtomicUsize::new(0),
            drop_oldest_enqueues: AtomicUsize::new(0),
            drop_oldest_oldest_discards: AtomicUsize::new(0),
            queued_handler_succeeded: AtomicUsize::new(0),
            queued_handler_failed: AtomicUsize::new(0),
            queued_handler_timed_out: AtomicUsize::new(0),
        }
    }

    /// Read all counters at once (point-in-time snapshot).
    pub fn snapshot(&self) -> TelemetrySnapshot {
        TelemetrySnapshot {
            messages_received: self.messages_received.load(Ordering::Relaxed),
            exact_route_hits: self.exact_route_hits.load(Ordering::Relaxed),
            prefix_route_hits: self.prefix_route_hits.load(Ordering::Relaxed),
            keyword_route_hits: self.keyword_route_hits.load(Ordering::Relaxed),
            fallback_dispatches: self.fallback_dispatches.load(Ordering::Relaxed),
            handler_dispatches: self.handler_dispatches.load(Ordering::Relaxed),
            reply_failures: self.reply_failures.load(Ordering::Relaxed),
            unmatched_messages: self.unmatched_messages.load(Ordering::Relaxed),
            handler_started: self.handler_started.load(Ordering::Relaxed),
            handler_succeeded: self.handler_succeeded.load(Ordering::Relaxed),
            handler_failed: self.handler_failed.load(Ordering::Relaxed),
            handler_timed_out: self.handler_timed_out.load(Ordering::Relaxed),
            drop_newest_drops: self.drop_newest_drops.load(Ordering::Relaxed),
            drop_oldest_enqueues: self.drop_oldest_enqueues.load(Ordering::Relaxed),
            drop_oldest_oldest_discards: self.drop_oldest_oldest_discards.load(Ordering::Relaxed),
            queued_handler_succeeded: self.queued_handler_succeeded.load(Ordering::Relaxed),
            queued_handler_failed: self.queued_handler_failed.load(Ordering::Relaxed),
            queued_handler_timed_out: self.queued_handler_timed_out.load(Ordering::Relaxed),
        }
    }

    /// Log all counters as a single INFO line.
    pub fn log_summary(&self) {
        let s = self.snapshot();
        tracing::info!(
            "Telemetry: msg_rcv={} exact={} prefix={} keyword={} fallback={} dispatch={} unmatched={} reply_err={} started={} ok={} err={} timeout={} drop_newest={} enqueue={} drop_oldest={} q_ok={} q_err={} q_timeout={}",
            s.messages_received, s.exact_route_hits, s.prefix_route_hits,
            s.keyword_route_hits, s.fallback_dispatches, s.handler_dispatches,
            s.unmatched_messages, s.reply_failures,
            s.handler_started, s.handler_succeeded, s.handler_failed, s.handler_timed_out,
            s.drop_newest_drops, s.drop_oldest_enqueues, s.drop_oldest_oldest_discards,
            s.queued_handler_succeeded, s.queued_handler_failed, s.queued_handler_timed_out,
        );
    }
}

impl Default for Telemetry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn t5_1_new_all_zero() {
        let t = Telemetry::new();
        let s = t.snapshot();
        assert_eq!(s.messages_received, 0);
        assert_eq!(s.exact_route_hits, 0);
        assert_eq!(s.handler_started, 0);
        assert_eq!(s.drop_newest_drops, 0);
    }

    #[test]
    fn t5_2_increment_and_snapshot() {
        let t = Telemetry::new();
        t.messages_received.fetch_add(1, Ordering::Relaxed);
        t.exact_route_hits.fetch_add(3, Ordering::Relaxed);
        t.handler_succeeded.fetch_add(5, Ordering::Relaxed);
        let s = t.snapshot();
        assert_eq!(s.messages_received, 1);
        assert_eq!(s.exact_route_hits, 3);
        assert_eq!(s.handler_succeeded, 5);
    }

    #[test]
    fn t5_3_default() {
        let t = Telemetry::default();
        let s = t.snapshot();
        assert_eq!(s.messages_received, 0);
    }

    #[test]
    fn t5_4_concurrent_increments() {
        let t = Arc::new(Telemetry::new());
        let mut handles = Vec::new();
        for _ in 0..10 {
            let tc = Arc::clone(&t);
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    tc.messages_received.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(t.messages_received.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn t5_5_log_summary_does_not_panic() {
        let t = Telemetry::new();
        t.messages_received.fetch_add(42, Ordering::Relaxed);
        t.log_summary();
    }

    #[test]
    fn t5_6_snapshot_default() {
        let s = TelemetrySnapshot::default();
        assert_eq!(s.messages_received, 0);
        assert_eq!(s.handler_dispatches, 0);
    }
}
