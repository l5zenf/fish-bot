use async_trait::async_trait;
use fish_core::AdapterEventSink;
use fish_core::error::Result;
use fish_core::message::MessageChain;
use fish_core::BaseAdapter;

struct MockAdapter;

#[async_trait]
impl BaseAdapter for MockAdapter {
    async fn send(&self, _target: &str, _msg: &MessageChain, _cid: Option<&str>) -> Result<()> {
        Ok(())
    }

    async fn run(&self, _sink: std::sync::Arc<dyn AdapterEventSink>) -> Result<()> {
        Ok(())
    }
}

#[test]
fn test_mock_adapter_compiles() {
    let _adapter = MockAdapter;
}
