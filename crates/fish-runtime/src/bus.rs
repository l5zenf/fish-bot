use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use parking_lot::RwLock;

use fish_core::error::{AppError, Result};

pub type BusPayload = Arc<dyn Any + Send + Sync>;
pub type BusFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
pub type BusHandler = Arc<dyn Fn(BusPayload) -> BusFuture + Send + Sync>;

pub(crate) trait ActorBus: Send + Sync + 'static {
    fn publish_raw(&self, topic: String, payload: BusPayload) -> BusFuture;

    fn subscribe_raw(&self, topic: String, handler: BusHandler);
}

#[derive(Clone)]
pub struct ActorBusHandle {
    inner: Arc<dyn ActorBus>,
}

impl ActorBusHandle {
    pub(crate) fn new(inner: Arc<dyn ActorBus>) -> Self {
        Self { inner }
    }

    pub(crate) fn runtime_default() -> Self {
        Self::new(Arc::new(RuntimeActorBus::default()))
    }

    pub async fn publish<T>(&self, topic: impl Into<String>, payload: T) -> Result<()>
    where
        T: Send + Sync + 'static,
    {
        self.inner
            .publish_raw(topic.into(), Arc::new(payload) as BusPayload)
            .await
    }

    pub fn subscribe<T, F, Fut>(&self, topic: impl Into<String>, handler: F)
    where
        T: Send + Sync + 'static,
        F: Fn(Arc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.inner.subscribe_raw(
            topic.into(),
            Arc::new(move |payload| {
                let handler = Arc::clone(&handler);
                Box::pin(async move {
                    let payload = payload
                        .downcast::<T>()
                        .map_err(|_| AppError::internal("actor bus payload type mismatch"))?;
                    handler(payload).await
                })
            }),
        );
    }
}

#[derive(Default)]
pub(crate) struct RuntimeActorBus {
    subscribers: RwLock<HashMap<String, Vec<BusHandler>>>,
}

impl ActorBus for RuntimeActorBus {
    fn publish_raw(&self, topic: String, payload: BusPayload) -> BusFuture {
        let subscribers = self
            .subscribers
            .read()
            .get(&topic)
            .cloned()
            .unwrap_or_default();

        Box::pin(async move {
            let mut payload = Some(payload);
            let total = subscribers.len();

            for (index, handler) in subscribers.into_iter().enumerate() {
                let current = if index + 1 == total {
                    payload
                        .take()
                        .expect("actor bus payload should exist for final subscriber")
                } else {
                    Arc::clone(
                        payload
                            .as_ref()
                            .expect("actor bus payload should exist while publishing"),
                    )
                };
                handler(current).await?;
            }

            Ok(())
        })
    }

    fn subscribe_raw(&self, topic: String, handler: BusHandler) {
        self.subscribers
            .write()
            .entry(topic)
            .or_default()
            .push(handler);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn runtime_actor_bus_delivers_typed_payload() -> anyhow::Result<()> {
        let bus = ActorBusHandle::new(Arc::new(RuntimeActorBus::default()));
        let seen = Arc::new(AtomicUsize::new(0));
        let seen_clone = Arc::clone(&seen);

        bus.subscribe("demo", move |payload: Arc<String>| {
            let seen = Arc::clone(&seen_clone);
            async move {
                if payload.as_str() == "hello" {
                    seen.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            }
        });

        bus.publish("demo", String::from("hello")).await?;
        assert_eq!(seen.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
