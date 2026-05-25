mod adapter;
mod api;
mod auth;
mod connection;
mod protocol;
mod sign;

pub use adapter::FishWebSocketAdapter;
pub use fish_runtime::{
    ActorBusHandle, ActorPluginBuilder, AdapterEventSink, AppError, BaseAdapter, Ctx,
    EventContext, MatchList, MessageChain, MessageChainItem, MessageContext, MessageEvent,
    MessageSegment, Plugin, QueueStrategy, Result, Rule, RuntimeHost, SystemEvent, Telemetry,
    is_fullmatch, is_keywords, is_regex, is_startswith, plugin,
};

pub mod prelude {
    pub use fish_runtime::prelude::*;
    pub use crate::FishWebSocketAdapter;
}

#[doc(hidden)]
pub mod __private {
    pub use fish_runtime::__private::*;
}
