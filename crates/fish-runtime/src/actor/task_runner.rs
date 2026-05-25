use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::runtime::{QueueStrategy, RuntimeConfig};
use crate::Result;
use fish_core::telemetry::Telemetry;

pub(super) type TaskFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

pub(super) struct PendingTask {
    pub(super) handler_id: String,
    pub(super) handler_timeout: Duration,
    pub(super) plugin_id: String,
    pub(super) future: TaskFuture,
    pub(super) telemetry: Arc<Telemetry>,
}

pub(super) struct TaskScheduler {
    semaphore: Arc<Semaphore>,
    strategy: QueueStrategy,
    pending_queue: Option<Arc<tokio::sync::Mutex<VecDeque<PendingTask>>>>,
    queue_notify: Option<Arc<tokio::sync::Notify>>,
    default_timeout: Duration,
}

impl TaskScheduler {
    pub(super) fn new(config: &RuntimeConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.concurrency));
        let strategy = config.queue_strategy.clone();
        let default_timeout = config.timeout;

        let (pending_queue, queue_notify) = match &strategy {
            QueueStrategy::DropNewest => (None, None),
            QueueStrategy::DropOldest(max_queue) => {
                let queue: Arc<tokio::sync::Mutex<VecDeque<PendingTask>>> =
                    Arc::new(tokio::sync::Mutex::new(VecDeque::with_capacity(*max_queue)));
                let notify = Arc::new(tokio::sync::Notify::new());
                spawn_queue_processor(Arc::clone(&queue), Arc::clone(&notify), Arc::clone(&semaphore));
                (Some(queue), Some(notify))
            }
        };

        Self {
            semaphore,
            strategy,
            pending_queue,
            queue_notify,
            default_timeout,
        }
    }

    pub(super) fn default_timeout(&self) -> Duration {
        self.default_timeout
    }

    pub(super) async fn dispatch_task_or_enqueue(
        &self,
        handler_id: &str,
        handler_timeout: Duration,
        plugin_id: &str,
        future: TaskFuture,
        telemetry: Arc<Telemetry>,
    ) {
        telemetry
            .handler_started
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let permit = match Arc::clone(&self.semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => match self.strategy {
                QueueStrategy::DropNewest => {
                    telemetry
                        .drop_newest_drops
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::warn!(
                        plugin = %plugin_id,
                        handler = %handler_id,
                        "plugin busy, dropping event"
                    );
                    return;
                }
                QueueStrategy::DropOldest(max_queue) => {
                    telemetry
                        .drop_oldest_enqueues
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::warn!(
                        plugin = %plugin_id,
                        handler = %handler_id,
                        "plugin busy, enqueuing event"
                    );
                    if let (Some(queue), Some(notify)) = (&self.pending_queue, &self.queue_notify) {
                        let task = PendingTask {
                            handler_id: handler_id.to_string(),
                            handler_timeout,
                            plugin_id: plugin_id.to_string(),
                            future,
                            telemetry: Arc::clone(&telemetry),
                        };
                        let mut q = queue.lock().await;
                        if q.len() >= max_queue {
                            telemetry
                                .drop_oldest_oldest_discards
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            q.pop_front();
                        }
                        q.push_back(task);
                        notify.notify_one();
                    }
                    return;
                }
            },
        };

        spawn_handler_task(
            permit,
            PendingTask {
                handler_id: handler_id.to_string(),
                handler_timeout,
                plugin_id: plugin_id.to_string(),
                future,
                telemetry,
            },
            false,
        );
    }
}

fn spawn_queue_processor(
    queue: Arc<tokio::sync::Mutex<VecDeque<PendingTask>>>,
    notify: Arc<tokio::sync::Notify>,
    semaphore: Arc<Semaphore>,
) {
    tokio::spawn(async move {
        loop {
            let task = {
                let mut q = queue.lock().await;
                q.pop_front()
            };

            match task {
                Some(task) => {
                    match Arc::clone(&semaphore).acquire_owned().await {
                        Ok(permit) => spawn_handler_task(permit, task, true),
                        Err(err) => {
                            tracing::warn!(error = %err, "task scheduler stopped accepting queued work");
                            break;
                        }
                    }
                }
                None => notify.notified().await,
            }
        }
    });
}

fn spawn_handler_task(
    permit: tokio::sync::OwnedSemaphorePermit,
    task: PendingTask,
    queued: bool,
) {
    tokio::spawn(async move {
        let _permit = permit;
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(task.handler_timeout, task.future).await;
        match result {
            Ok(Ok(())) => {
                let counter = if queued {
                    &task.telemetry.queued_handler_succeeded
                } else {
                    &task.telemetry.handler_succeeded
                };
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::debug!(
                    plugin = %task.plugin_id,
                    handler = %task.handler_id,
                    cost_ms = started.elapsed().as_millis(),
                    "handler finished"
                );
            }
            Ok(Err(err)) => {
                let counter = if queued {
                    &task.telemetry.queued_handler_failed
                } else {
                    &task.telemetry.handler_failed
                };
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    plugin = %task.plugin_id,
                    handler = %task.handler_id,
                    error = %err,
                    cost_ms = started.elapsed().as_millis(),
                    "handler failed"
                );
            }
            Err(_) => {
                let counter = if queued {
                    &task.telemetry.queued_handler_timed_out
                } else {
                    &task.telemetry.handler_timed_out
                };
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    plugin = %task.plugin_id,
                    handler = %task.handler_id,
                    timeout_ms = task.handler_timeout.as_millis(),
                    "handler timeout"
                );
            }
        }
    });
}
