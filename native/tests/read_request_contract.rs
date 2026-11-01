// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::observation::ReadRequest;
use serde_json::Value;

#[test]
fn every_generated_qualified_read_has_a_closed_roundtrip_but_effects_and_properties_do_not() {
    let catalog: Value = serde_json::from_str(include_str!("../assets/tools.json")).unwrap();
    let requests = catalog["requests"].as_object().unwrap();
    assert_eq!(requests.len(), 59);
    for entry in requests.values() {
        let name = entry["requestType"].as_str().unwrap();
        assert_eq!(ReadRequest::from_name(name).unwrap().as_str(), name);
    }
    for name in [
        "SetInputMute",
        "StartRecord",
        "CallVendorRequest",
        "GetInputPropertiesListPropertyItems",
        "GetInputPropertiesListPropertyItem",
        "GetUnknown",
    ] {
        assert_eq!(
            ReadRequest::from_name(name),
            None,
            "unqualified request {name} entered the read vocabulary"
        );
    }
}
