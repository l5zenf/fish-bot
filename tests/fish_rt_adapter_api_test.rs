use fish_runtime::FishWebSocketAdapter as RuntimeFishWebSocketAdapter;
use fish_rt_adapter::FishWebSocketAdapter as AdapterFishWebSocketAdapter;

#[test]
fn fish_adapter_is_available_from_new_crate_and_runtime_compat_layer() {
    let _from_runtime = RuntimeFishWebSocketAdapter::new();
    let _from_adapter = AdapterFishWebSocketAdapter::new();
}
