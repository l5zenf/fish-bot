use std::sync::Arc;

use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_core::error::Result;
use fish_core::event::{MessageEvent, SystemEvent};
use fish_core::message::{MessageChain, MessageSegment};
use fish_core::telemetry::Telemetry;
use fish_plugin::plugin::HandlerContext;

/// Ergonomics wrapper around `HandlerContext` for plugin handlers.
///
/// Provides convenient methods like `reply()`, `sender_id()`, `text()`
/// so plugin authors don't need to reach into the raw `HandlerContext` fields.
///
/// For event handlers (`#[event]`), create via `Context::new_from_event()`.
/// In that case `reply()`, `sender_id()`, and `text()` are not available
/// (they will return errors / panics).
pub struct Context {
    pub(crate) inner: ContextInner,
}

pub(crate) enum ContextInner {
    #[allow(dead_code)]
    Message(HandlerContext),
}

impl Context {
    /// Create from a handler context (message handlers).
    pub(crate) fn new(inner: HandlerContext) -> Self {
        Self {
            inner: ContextInner::Message(inner),
        }
    }

    /// Create from system event + adapter + ctx (event handlers).
    #[allow(unused_variables)]
    pub(crate) fn new_from_event(
        event: Arc<SystemEvent>,
        adapter: Arc<dyn BaseAdapter>,
        app_ctx: Arc<Ctx>,
    ) -> Self {
        Self {
            inner: ContextInner::Message(HandlerContext {
                event: MessageEvent::new(
                    String::new(),
                    String::new(),
                    String::new(),
                    MessageChain::new(),
                    serde_json::Value::Null,
                ),
                adapter,
                app_ctx,
                telemetry: Arc::new(Telemetry::new()),
                plugin_state: None,
            }),
        }
    }

    /// Reply with a text message to the event sender.
    /// Available only for message handlers (`#[command]`, `#[message]`).
    /// Panics if called in an event handler context.
    pub async fn reply(&self, text: impl Into<String>) -> Result<()> {
        match &self.inner {
            ContextInner::Message(hc) => {
                hc.event.reply(MessageSegment::text(text.into())).await;
                Ok(())
            }
        }
    }

    /// The sender's user ID. Available for message handlers only.
    pub fn sender_id(&self) -> &str {
        match &self.inner {
            ContextInner::Message(hc) => &hc.event.sender_id,
        }
    }

    /// The conversation / channel ID. Available for message handlers only.
    pub fn cid(&self) -> &str {
        match &self.inner {
            ContextInner::Message(hc) => &hc.event.cid,
        }
    }

    /// The plain text content of the message. Available for message handlers only.
    pub fn text(&self) -> String {
        match &self.inner {
            ContextInner::Message(hc) => hc.event.plain_text(),
        }
    }

    /// Access the adapter for sending messages.
    pub fn adapter(&self) -> &Arc<dyn BaseAdapter> {
        match &self.inner {
            ContextInner::Message(hc) => &hc.adapter,
        }
    }

    /// Access the shared application context.
    pub fn app_ctx(&self) -> &Arc<Ctx> {
        match &self.inner {
            ContextInner::Message(hc) => &hc.app_ctx,
        }
    }

    /// Access the raw message event (message handlers).
    pub fn event(&self) -> &MessageEvent {
        match &self.inner {
            ContextInner::Message(hc) => &hc.event,
        }
    }

    /// Access telemetry counters.
    pub fn telemetry(&self) -> &Arc<Telemetry> {
        match &self.inner {
            ContextInner::Message(hc) => &hc.telemetry,
        }
    }
}
