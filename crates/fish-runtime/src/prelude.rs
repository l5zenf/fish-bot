//! Prelude — glob import the most commonly used types.
//!
//! ```rust
//! use fish_runtime::prelude::*;
//! ```

pub use crate::{
    AdapterEventSink, ActorMailbox, ActorPlugin, ActorPluginBuilder, AppError, BaseAdapter, Ctx, EventContext, EventHandler,
    EventHandlerContext, EventHandlerFunc, EventHandlerFuture, HandlerContext, HandlerFunc,
    HandlerFuture, MessageChain, MessageChainItem, MessageContext, MessageEvent, MessageHandler,
    MessageSegment, Plugin, PluginMetadata, PluginState, QueueStrategy, Result,
    RouteHint, Rule, RuntimeHost, SystemEvent, Telemetry, is_fullmatch, is_keywords, is_regex,
    is_startswith,
};
