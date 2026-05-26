mod adapter;
mod api;
mod auth;
mod client;
mod connection;
mod sign;

pub use adapter::FishWebSocketAdapter;
pub use auth::{CookieImportReport, import_browser_cookies};
pub use client::{ClientProvider, FishHttpClient};
pub use fish_runtime::{
    ActorBusHandle, ActorPluginBuilder, AdapterEventSink, AppError, BaseAdapter, Ctx, EventContext,
    MatchList, MessageChain, MessageChainItem, MessageContext, MessageEvent, MessageSegment,
    Plugin, QueueStrategy, Result, Rule, RuntimeHost, SystemEvent, Telemetry, is_fullmatch,
    is_keywords, is_regex, is_startswith, plugin,
};

pub mod prelude {
    pub use crate::FishWebSocketAdapter;
    pub use fish_runtime::prelude::*;
}

#[doc(hidden)]
pub mod __private {
    pub use fish_runtime::__private::*;
}
