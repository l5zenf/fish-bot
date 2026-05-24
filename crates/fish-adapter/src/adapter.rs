use fish_core::error::Result;
use fish_core::event::{MessageEvent, SystemEvent};
use fish_core::message::MessageChain;
use async_trait::async_trait;


/// BaseAPI trait — marker trait for API clients.
pub trait BaseAPI: Send + Sync {}

/// Base adapter trait.
#[async_trait]
pub trait BaseAdapter: Send + Sync {
    /// Set the callback invoked when a MessageEvent is received.
    fn set_callback(&self, cb: Box<dyn Fn(MessageEvent) + Send + Sync>);

    /// Set the callback invoked when a SystemEvent (non-chat) is received.
    /// Default no-op implementation for adapters that don't support system events.
    fn set_event_callback(&self, _cb: Box<dyn Fn(SystemEvent) + Send + Sync>) {}

    /// Send a message through the adapter.
    async fn send(
        &self,
        target_id: &str,
        message: &MessageChain,
        cid: Option<&str>,
    ) -> Result<()>;

    /// Start the adapter event loop (blocks until shutdown).
    async fn run(&self) -> Result<()>;
}
