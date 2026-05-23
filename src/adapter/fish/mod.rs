pub mod sign;
pub mod auth;
pub mod api;

use crate::adapter::BaseAdapter;
use crate::error::Result;
use crate::model::{Message, MessageEvent};

pub struct FishWebSocketAdapter;

impl FishWebSocketAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl BaseAdapter for FishWebSocketAdapter {
    fn set_callback(&self, _cb: Box<dyn Fn(MessageEvent) + Send + Sync>) {}

    async fn send(&self, _target_id: &str, _message: &Message, _cid: Option<&str>) -> Result<()> {
        todo!()
    }

    async fn run(&self) -> Result<()> {
        // 1. ensure auth
        // 2. connect WS
        // 3. init connection
        // 4. start heartbeat
        // 5. receive loop with auto-reconnect
        todo!()
    }
}
