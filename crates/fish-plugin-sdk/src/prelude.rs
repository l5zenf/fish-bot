//! Prelude — glob import the most commonly used types.
//!
//! ```rust
//! use fish_plugin_sdk::prelude::*;
//! ```

pub use crate::{
    AppError, BaseAdapter, BuiltPlugin, Ctx, EventHandler, EventHandlerFunc, EventHandlerFuture,
    HandlerContext, HandlerFunc, HandlerFuture, MessageChain, MessageChainItem,
    MessageEvent, MessageHandler, MessageSegment, Plugin, PluginBuilder, PluginMetadata,
    QueueStrategy, Result, RouteHint, Rule, SystemEvent, Telemetry,
    is_fullmatch, is_keywords, is_regex, is_startswith, register_plugin, registered_plugins,
};
