use fish_rt_adapter::FishWebSocketAdapter as AdapterFishWebSocketAdapter;
use fish_rt_adapter::RuntimeHost;
use fish_rt_adapter::plugin;
use fish_rt_adapter::prelude::*;
use std::fs;

#[test]
fn fish_adapter_is_available_from_adapter_facade() {
    let _from_adapter = AdapterFishWebSocketAdapter::new();
    let _runtime_type = std::mem::size_of::<Option<RuntimeHost>>();
    let _plugin = AdapterFacadePlugin;
}

#[test]
fn examples_default_to_fish_websocket_adapter() {
    let simple = fs::read_to_string("examples/quickstart-simple/src/app/bootstrap.rs")
        .expect("read quickstart-simple bootstrap");
    let custom = fs::read_to_string("examples/quickstart-custom/src/app/bootstrap.rs")
        .expect("read quickstart-custom bootstrap");

    assert!(simple.contains("FishWebSocketAdapter"));
    assert!(custom.contains("FishWebSocketAdapter"));
    assert!(!simple.contains("LocalAdapter"));
    assert!(!custom.contains("LocalAdapter"));
}

#[test]
fn examples_accept_cookie_cli_flag() {
    let simple = fs::read_to_string("examples/quickstart-simple/src/main.rs")
        .expect("read quickstart-simple main");
    let custom = fs::read_to_string("examples/quickstart-custom/src/main.rs")
        .expect("read quickstart-custom main");

    assert!(simple.contains("clap"));
    assert!(custom.contains("clap"));
    assert!(simple.contains("cookies"));
    assert!(custom.contains("cookies"));
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
