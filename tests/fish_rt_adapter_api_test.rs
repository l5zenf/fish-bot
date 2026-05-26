use fish_rt_adapter::FishWebSocketAdapter as AdapterFishWebSocketAdapter;
use fish_rt_adapter::{BaseAdapter, ClientProvider, Ctx, FishHttpClient, RuntimeHost, Telemetry};
use fish_rt_adapter::plugin;
use fish_rt_adapter::prelude::*;
use std::fs;
use std::sync::Arc;

#[test]
fn fish_adapter_is_available_from_adapter_facade() {
    let _from_adapter = AdapterFishWebSocketAdapter::new();
    let _runtime_type = std::mem::size_of::<Option<RuntimeHost>>();
    let _plugin = AdapterFacadePlugin;
}

#[test]
fn fish_client_types_are_available_from_adapter_facade() {
    let _client = std::mem::size_of::<Option<FishHttpClient>>();
    let _trait_ref: Option<&dyn ClientProvider<Client = reqwest::Client>> = None;
}

#[test]
fn runtime_host_injects_fish_http_client() {
    let adapter: Arc<dyn BaseAdapter> = Arc::new(AdapterFishWebSocketAdapter::new());
    let ctx = Arc::new(Ctx::new());

    let _host = RuntimeHost::new(adapter, vec![], Arc::clone(&ctx), Arc::new(Telemetry::new()));

    let fish = ctx
        .get::<FishHttpClient>()
        .expect("fish http client should be registered");
    let _client = fish.client();
    let _client_ref: &reqwest::Client = fish.client_ref();
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
