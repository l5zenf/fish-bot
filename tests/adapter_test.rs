use async_trait::async_trait;
use fish_adapter::adapter::{BaseAdapter, BaseAPI};
use fish_core::error::Result;
use fish_core::event::MessageEvent;
use fish_core::message::MessageChain;

struct MockAdapter;

#[async_trait]
impl BaseAdapter for MockAdapter {
    fn set_callback(&self, _cb: Box<dyn Fn(MessageEvent) + Send + Sync>) {}

    async fn send(
        &self,
        _target: &str,
        _msg: &MessageChain,
        _cid: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }

    async fn run(&self) -> Result<()> {
        Ok(())
    }
}

struct MockApi;

impl BaseAPI for MockApi {}

#[test]
fn test_mock_adapter_compiles() {
    let _adapter = MockAdapter;
    let _api = MockApi;
}
