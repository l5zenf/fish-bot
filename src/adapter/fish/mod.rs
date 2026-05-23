use std::pin::Pin;
use std::future::Future;
use crate::adapter::BaseAdapter;
use crate::error::Result;
use crate::model::{Message, MessageEvent};

pub mod sign;
pub mod auth;
pub mod api;

pub struct FishWebSocketAdapter;

impl FishWebSocketAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl BaseAdapter for FishWebSocketAdapter {
    fn set_callback(&self, _cb: Box<dyn Fn(MessageEvent) + Send + Sync>) {}

    fn send<'a>(
        &'a self,
        _target_id: &'a str,
        _message: &'a Message,
        _cid: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            todo!()
        })
    }

    fn run<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        // 1. ensure auth
        // 2. connect WS
        // 3. init connection
        // 4. start heartbeat
        // 5. receive loop with auto-reconnect
        Box::pin(async move {
            todo!()
        })
    }
}
