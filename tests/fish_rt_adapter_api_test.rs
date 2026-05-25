use fish_rt_adapter::FishWebSocketAdapter as AdapterFishWebSocketAdapter;
use fish_rt_adapter::RuntimeHost;
use fish_rt_adapter::plugin;
use fish_rt_adapter::prelude::*;

#[test]
fn fish_adapter_is_available_from_adapter_facade() {
    let _from_adapter = AdapterFishWebSocketAdapter::new();
    let _runtime_type = std::mem::size_of::<Option<RuntimeHost>>();
    let _plugin = AdapterFacadePlugin;
}

#[allow(dead_code)]
struct AdapterFacadePlugin;

#[plugin]
#[allow(dead_code)]
impl AdapterFacadePlugin {
    #[message("/ping")]
    async fn ping(&self, _ctx: MessageContext) -> Result<()> {
        Ok(())
    }
}
