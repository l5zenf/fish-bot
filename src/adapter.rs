use std::future::Future;
use std::pin::Pin;
use crate::error::Result;
use crate::model::{Message, MessageEvent};

pub mod fish;

pub trait BaseAdapter: Send + Sync {
    fn set_callback(&self, cb: Box<dyn Fn(MessageEvent) + Send + Sync>);
    fn send<'a>(
        &'a self,
        target_id: &'a str,
        message: &'a Message,
        cid: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
    fn run<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}
