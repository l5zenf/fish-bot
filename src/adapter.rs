use crate::error::Result;
use crate::model::{Message, MessageEvent};

pub mod fish;

pub trait BaseAdapter: Send + Sync {
    fn set_callback(&self, cb: Box<dyn Fn(MessageEvent) + Send + Sync>);
    fn send(&self, target_id: &str, message: &Message, cid: Option<&str>) -> impl std::future::Future<Output = Result<()>> + Send;
    fn run(&self) -> impl std::future::Future<Output = Result<()>> + Send;
}
