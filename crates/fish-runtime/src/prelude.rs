//! Prelude — glob import the most commonly used types.
//!
//! ```rust
//! use fish_runtime::prelude::*;
//! ```

pub use crate::{
    AdapterEventSink, AppError, BaseAdapter, BuiltPlugin, Context, Ctx, EventHandler,
    EventHandlerContext, EventHandlerFunc, EventHandlerFuture, HandlerContext, HandlerFunc,
    HandlerFuture, MessageChain, MessageChainItem, MessageEvent, MessageHandler, MessageSegment,
    Plugin, PluginBuilder, PluginMetadata, QueueStrategy, Result, RouteHint, Rule, RuntimeHost,
    SystemEvent, Telemetry, is_fullmatch, is_keywords, is_regex, is_startswith,
};
