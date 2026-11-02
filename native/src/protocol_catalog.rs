// SPDX-License-Identifier: GPL-2.0-only
use std::{collections::HashMap, sync::OnceLock};

use serde_json::{Value, json};

struct Catalog {
    requests: Vec<Value>,
    by_type: HashMap<String, usize>,
}

impl Catalog {
    fn parse(document: &str) -> Result<Self, &'static str> {
        let mut document: Value =
            serde_json::from_str(document).map_err(|_| "docs/protocol.json must be valid JSON")?;
        let requests = document
            .get_mut("requests")
            .and_then(Value::as_array_mut)
            .map(std::mem::take)
            .ok_or("docs/protocol.json must contain a requests array")?;
        let mut by_type = HashMap::with_capacity(requests.len());
        for (index, request) in requests.iter().enumerate() {
            if !request.is_object()
                || !["requestType", "description", "category", "initialVersion"]
                    .iter()
                    .all(|field| request[*field].is_string())
                || !request["deprecated"].is_boolean()
                || !request["requestFields"].is_array()
                || !request["responseFields"].is_array()
            {
                return Err("docs/protocol.json contains an invalid request definition");
            }
            // TS constructs a Map from the ordered source entries: duplicate
            // request types retain their last definition for exact lookup.
            by_type.insert(request["requestType"].as_str().unwrap().to_owned(), index);
        }
        Ok(Self { requests, by_type })
    }

    fn get(&self, request_type: &str) -> Option<&Value> {
        self.by_type
            .get(request_type)
            .map(|index| &self.requests[*index])
    }

    fn list(&self, category: Option<&str>) -> Value {
        let category = category
            .map(str::to_lowercase)
            .filter(|value| !value.is_empty());
        Value::Array(
            self.requests
                .iter()
                .filter(|request| {
                    category.as_ref().is_none_or(|category| {
                        request["category"].as_str().unwrap().to_lowercase() == *category
                    })
                })
                .map(|request| {
                    json!({
                        "requestType": request["requestType"],
                        "category": request["category"],
                        "description": request["description"],
                        "deprecated": request["deprecated"],
                        "initialVersion": request["initialVersion"],
                    })
                })
                .collect(),
        )
    }
}

fn catalog() -> &'static Catalog {
    static CATALOG: OnceLock<Catalog> = OnceLock::new();
    CATALOG.get_or_init(|| {
        Catalog::parse(include_str!("../../docs/protocol.json"))
            .expect("checked embedded OBS protocol catalog")
    })
}

fn result(payload: Value) -> Value {
    // Match withStructuredToolResults: arrays are wrapped, objects remain
    // objects. JSON text key order is not part of the protocol contract.
    let text = serde_json::to_string_pretty(&payload).expect("JSON catalog is serializable");
    let structured = if payload.is_object() {
        payload
    } else {
        json!({ "result": payload })
    };
    json!({ "content": [{ "type": "text", "text": text }], "structuredContent": structured })
}

/// Arguments have already passed the tool's shared schema normalization.
/// This operation owns immutable catalog data and never accesses OBS.
pub(crate) fn describe(arguments: &Value) -> Value {
    if let Some(request_type) = arguments["requestType"]
        .as_str()
        .filter(|value| !value.is_empty())
    {
        return match catalog().get(request_type) {
            Some(request) => result(request.clone()),
            None => json!({
                "content": [{ "type": "text", "text": format!("Unknown OBS WebSocket request: {request_type}") }],
                "isError": true,
            }),
        };
    }
    result(catalog().list(arguments["category"].as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_catalog_preserves_full_exact_definition_and_list_order() {
        let source: Value = serde_json::from_str(include_str!("../../docs/protocol.json")).unwrap();
        let expected = source["requests"]
            .as_array()
            .unwrap()
            .iter()
            .find(|request| request["requestType"] == "GetVersion")
            .unwrap();
        assert_eq!(
            describe(&json!({ "requestType": "GetVersion" }))["structuredContent"],
            *expected
        );
        let listed = describe(&json!({}));
        let entries = listed["structuredContent"]["result"].as_array().unwrap();
        assert_eq!(entries.len(), source["requests"].as_array().unwrap().len());
        assert_eq!(
            entries[0]["requestType"],
            source["requests"][0]["requestType"]
        );
        assert!(
            entries
                .iter()
                .all(|request| request.as_object().unwrap().len() == 5)
        );
    }

    #[test]
    fn lookup_is_case_sensitive_and_unknown_errors_have_no_structured_content() {
        let unknown = describe(&json!({ "requestType": "getversion", "category": "General" }));
        assert_eq!(
            unknown,
            json!({
                "content": [{ "type": "text", "text": "Unknown OBS WebSocket request: getversion" }],
                "isError": true,
            })
        );
    }

    #[test]
    fn empty_request_lists_and_category_folds_case_without_trimming() {
        let expected = describe(&json!({ "category": "General" }));
        assert_eq!(
            describe(&json!({ "requestType": "", "category": "gEnErAl" })),
            expected
        );
        assert_eq!(
            describe(&json!({ "category": " General " }))["structuredContent"],
            json!({ "result": [] })
        );
        assert_eq!(describe(&json!({ "category": "" })), describe(&json!({})));
    }

    #[test]
    fn validation_rejects_invalid_catalogs_and_lookup_matches_last_map_entry() {
        assert!(Catalog::parse("{}").is_err());
        assert!(Catalog::parse("{\"requests\":[{}]}").is_err());
        let request = |description: &str| {
            json!({
                "requestType": "Duplicate", "description": description,
                "category": "General", "deprecated": false, "initialVersion": "5.0.0",
                "requestFields": [], "responseFields": [], "extension": true,
            })
        };
        let parsed =
            Catalog::parse(&json!({ "requests": [request("first"), request("last")] }).to_string())
                .unwrap();
        assert_eq!(parsed.get("Duplicate").unwrap(), &request("last"));
        assert_eq!(parsed.list(None).as_array().unwrap().len(), 2);
    }
}
