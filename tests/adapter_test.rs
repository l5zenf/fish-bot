use fish_bot::adapter::BaseAdapter;
use fish_bot::model::Message;

struct MockAdapter;

impl BaseAdapter for MockAdapter {
    fn set_callback(&self, _cb: Box<dyn Fn(fish_bot::model::MessageEvent) + Send + Sync>) {}

    async fn send(&self, _target: &str, _msg: &Message, _cid: Option<&str>) -> fish_bot::error::Result<()> {
        Ok(())
    }

    async fn run(&self) -> fish_bot::error::Result<()> {
        Ok(())
    }
}
