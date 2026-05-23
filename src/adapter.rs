use crate::error::Result;
use crate::model::{Message, MessageEvent};
use async_trait::async_trait;

pub mod fish;

#[async_trait]
pub trait BaseAdapter: Send + Sync {
    fn set_callback(&self, cb: Box<dyn Fn(MessageEvent) + Send + Sync>);
    async fn send(&self, target_id: &str, message: &Message, cid: Option<&str>) -> Result<()>;
    async fn run(&self) -> Result<()>;
}
