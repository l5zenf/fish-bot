use fish_bot::adapter::BaseAdapter;
use fish_bot::model::Message;
use async_trait::async_trait;

struct MockAdapter;

#[async_trait]
impl BaseAdapter for MockAdapter {
    fn set_callback(&self, _cb: Box<dyn Fn(fish_bot::model::MessageEvent) + Send + Sync>) {}

    async fn send(&self, _target: &str, _msg: &Message, _cid: Option<&str>) -> fish_bot::error::Result<()> {
        Ok(())
    }

    async fn run(&self) -> fish_bot::error::Result<()> {
        Ok(())
    }
}
