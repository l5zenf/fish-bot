//! Prelude — glob import the most commonly used types.
//!
//! ```rust
//! use fish_runtime::prelude::*;
//! ```

pub use crate::plugin::PluginMetadata;
pub use crate::runtime::QueueStrategy;
pub use crate::{
    ActorBus, ActorBusHandle, ActorMailbox, ActorPlugin, ActorPluginBuilder, AdapterEventSink,
    AppError, BaseAdapter, Ctx, EventContext, MessageChain, MessageChainItem, MessageContext,
    MessageEvent, MessageSegment, Plugin, Result, RuntimeActorBus, RuntimeHost, SystemEvent,
    Telemetry,
};
