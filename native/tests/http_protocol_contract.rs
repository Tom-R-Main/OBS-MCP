// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::http_protocol::{
    self as http, Admission, Era, ErrorOrigin, Policy, RequestHead, ResponseMode, Unqualified,
};
use serde_json::{Value, json};

#[test]
fn actual_source_http_boundary_vectors() {
    let rows: Value = serde_json::from_str(include_str!("http_source_vectors.json")).unwrap();
    let mut failures = Vec::new();
    for row in rows.as_array().unwrap() {
        let request = &row["request"];
        let mut headers: Vec<(String, String)> = request["headers"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
            .collect();
        match request["authorization"].as_str().unwrap() {
            "correct" => headers.push(("authorization".into(), "Bearer fixture-http-token".into())),
            "wrong" => headers.push(("authorization".into(), "Bearer fixture-wrong-token".into())),
            "lowercase" => {
                headers.push(("authorization".into(), "bearer fixture-http-token".into()))
            }
            "equal-length-wrong" => {
                headers.push(("authorization".into(), "Bearer fixture-http-tokex".into()))
            }
            _ => {}
        }
        let headers: Vec<(&str, &str)> = headers
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        let policy = Policy::new(
            request["token"]
                .as_bool()
                .unwrap()
                .then_some("fixture-http-token"),
        );
        let original = request["body"].as_str().unwrap().as_bytes().to_vec();
        let admission = http::admit(
            RequestHead {
                method: request["method"].as_str().unwrap(),
                target: request["target"].as_str().unwrap(),
                headers: &headers,
            },
            original.clone(),
            &policy,
        );
        let observed = match admission {
            Admission::Reply(reply) => {
                let mut result = json!({"status":reply.status,"content_type":reply.content_type,"authenticate_bearer":reply.authenticate_bearer});
                if row["expected"].get("error").is_some() {
                    result["error"] = serde_json::from_slice(&reply.body).unwrap();
                }
                if row["expected"].get("text").is_some() {
                    result["text"] = String::from_utf8(reply.body).unwrap().into();
                }
                result
            }
            Admission::Exchange(exchange) => {
                assert_eq!(
                    exchange.original_body(),
                    original,
                    "{} reordered body",
                    row["name"]
                );
                let mode = match exchange.response_mode() {
                    ResponseMode::Json => "json",
                    ResponseMode::Sse => "sse",
                    ResponseMode::Empty => "empty",
                    ResponseMode::Listen => "listen",
                };
                let (status, content_type) = if mode == "empty" {
                    (202, None)
                } else {
                    (
                        200,
                        Some(if mode == "json" {
                            "application/json"
                        } else {
                            "text/event-stream"
                        }),
                    )
                };
                let mut result = json!({"status":status,"content_type":content_type,"authenticate_bearer":false});
                if row["expected"].get("mode").is_some() {
                    result["mode"] = mode.into();
                }
                // Tool-registry errors belong to the host after admission;
                // feed the actual Source payload through the response seam.
                if let Some(payload) = row["expected"].get("error") {
                    let reply = exchange.respond(payload, ErrorOrigin::Handler);
                    result["status"] = reply.status.into();
                    result["error"] = serde_json::from_slice::<Value>(&reply.body).unwrap();
                }
                result
            }
            Admission::Unqualified(reason) => json!({"unqualified":format!("{reason:?}")}),
        };
        if observed != row["expected"] {
            failures
                .push(json!({"case":row["name"],"expected":row["expected"],"observed":observed}));
        }
    }
    assert!(
        failures.is_empty(),
        "{}",
        serde_json::to_string_pretty(&failures).unwrap()
    );
}

fn modern(target: &str) -> http::Exchange {
    let headers = [
        ("host", "127.0.0.1:4455"),
        ("content-type", "application/json"),
        ("mcp-protocol-version", http::MODERN),
        ("mcp-method", "tools/call"),
        ("mcp-name", "fixture"),
    ];
    let body=br#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"fixture","arguments":{"z":1,"a":2},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{"elicitation":{"form":{}}}}}}"#.to_vec();
    match http::admit(
        RequestHead {
            method: "POST",
            target,
            headers: &headers,
        },
        body,
        &Policy::new(None),
    ) {
        Admission::Exchange(exchange) => exchange,
        _ => panic!("expected modern exchange"),
    }
}

#[test]
fn exchange_context_is_stateless_and_legacy_cannot_borrow_capability_offers() {
    let exchange = modern("/mcp?client=spoofed");
    assert_eq!(exchange.era(), Era::Modern);
    assert!(exchange.legacy_declared_client().is_none());
    assert_eq!(
        exchange.client_capabilities(),
        Some(&json!({"elicitation":{"form":{}}}))
    );
    let headers = [
        ("host", "localhost"),
        ("content-type", "application/json"),
        ("accept", "application/json, text/event-stream"),
    ];
    let initialize = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{"elicitation":{"form":{}}},"clientInfo":{"name":"old","version":"1"}}});
    for message in [
        initialize,
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"fixture","arguments":{}}}),
    ] {
        let Admission::Exchange(exchange) = http::admit(
            RequestHead {
                method: "POST",
                target: "/mcp?client=%20old.agent%20",
                headers: &headers,
            },
            serde_json::to_vec(&message).unwrap(),
            &Policy::new(None),
        ) else {
            panic!("legacy exchange")
        };
        assert_eq!(exchange.era(), Era::Legacy);
        assert_eq!(exchange.legacy_declared_client(), Some("old.agent"));
        assert!(exchange.client_capabilities().is_none());
    }
}

#[test]
fn error_origin_and_sse_payload_are_separate_from_http_transport_framing() {
    let exchange = modern("/mcp");
    let payload = json!({"jsonrpc":"2.0","id":7,"error":{"code":-32601,"message":"missing"}});
    assert_eq!(
        exchange.respond(&payload, ErrorOrigin::Protocol).status,
        404
    );
    assert_eq!(exchange.respond(&payload, ErrorOrigin::Handler).status, 200);
    let response = http::Response::sse(200, &payload);
    assert_eq!(response.content_type, Some("text/event-stream"));
    let data = std::str::from_utf8(&response.body).unwrap();
    assert!(data.starts_with("event: message\ndata: ") && data.ends_with("\n\n"));
    assert_eq!(
        serde_json::from_str::<Value>(
            data.strip_prefix("event: message\ndata: ")
                .unwrap()
                .trim_end()
        )
        .unwrap(),
        payload
    );
}

#[test]
fn body_header_and_url_bounds_refuse_without_observable_effect_authority() {
    let headers = [("host", "localhost"), ("content-type", "application/json")];
    let Admission::Reply(response) = http::admit(
        RequestHead {
            method: "POST",
            target: "/mcp",
            headers: &headers,
        },
        vec![b' '; http::MAX_BODY_BYTES + 1],
        &Policy::new(None),
    ) else {
        panic!("oversize")
    };
    assert_eq!(response.status, 413);
    let headers = [("host", "localhost"), ("host", "evil.example")];
    assert!(matches!(
        http::admit(
            RequestHead {
                method: "POST",
                target: "/mcp",
                headers: &headers
            },
            vec![],
            &Policy::new(None)
        ),
        Admission::Unqualified(Unqualified::DuplicateHeader)
    ));
    let headers = [("host", "127.1")];
    // WHATWG would canonicalize this to127.0.0.1; this slice deliberately
    // refuses instead of claiming equivalent URL normalization.
    assert!(matches!(
        http::admit(
            RequestHead {
                method: "POST",
                target: "/mcp",
                headers: &headers
            },
            vec![],
            &Policy::new(None)
        ),
        Admission::Reply(_)
    ));
}

#[test]
fn unsupported_capability_shapes_cannot_become_confirmation_authority() {
    let base = modern("/mcp");
    let headers = [
        ("host", "localhost"),
        ("content-type", "application/json"),
        ("mcp-protocol-version", http::MODERN),
        ("mcp-method", "tools/call"),
        ("mcp-name", "fixture"),
    ];
    for capabilities in [
        json!({"elicitation":{"form":false}}),
        json!({"elicitation":{"form":{"applyDefaults":"true"}}}),
        json!({"futureCapability":{}}),
        json!({"roots":{"listChanged":"yes"}}),
    ] {
        let mut message = base.message().clone();
        message["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"] = capabilities;
        assert!(matches!(
            http::admit(
                RequestHead {
                    method: "POST",
                    target: "/mcp",
                    headers: &headers
                },
                serde_json::to_vec(&message).unwrap(),
                &Policy::new(None)
            ),
            Admission::Unqualified(Unqualified::EnvelopeSchema)
        ));
    }
}

#[test]
fn method_refusal_advertises_post_with_the_complete_source_error() {
    for version in [None, Some(http::MODERN)] {
        let mut headers = vec![("host", "localhost")];
        if let Some(version) = version {
            headers.push(("mcp-protocol-version", version));
        }
        for method in ["GET", "PUT"] {
            let Admission::Reply(reply) = http::admit(
                RequestHead {
                    method,
                    target: "/mcp",
                    headers: &headers,
                },
                Vec::new(),
                &Policy::new(None),
            ) else {
                panic!("unsupported method was admitted");
            };
            assert_eq!(reply.status, 405);
            assert_eq!(reply.allow_header(), Some("POST"));
            assert_eq!(reply.content_type, Some("application/json"));
            assert!(!reply.authenticate_bearer);
            assert_eq!(
                serde_json::from_slice::<Value>(&reply.body).unwrap(),
                json!({
                    "jsonrpc":"2.0","error":{"code":-32000,"message":"Method not allowed."},"id":null
                })
            );
        }
    }
}

#[test]
fn earlier_path_auth_and_origin_refusals_do_not_advertise_methods() {
    for version in [None, Some(http::MODERN)] {
        for (target, origin, token, status) in [
            ("/wrong", None, None, 404),
            ("/mcp", None, Some("fixture-token"), 401),
            (
                "/mcp",
                Some("http://other.invalid"),
                Some("fixture-token"),
                403,
            ),
        ] {
            let mut headers = vec![("host", "localhost")];
            if let Some(version) = version {
                headers.push(("mcp-protocol-version", version));
            }
            if let Some(origin) = origin {
                headers.push(("origin", origin));
            }
            let Admission::Reply(reply) = http::admit(
                RequestHead {
                    method: "GET",
                    target,
                    headers: &headers,
                },
                Vec::new(),
                &Policy::new(token),
            ) else {
                panic!("earlier refusal was admitted");
            };
            assert_eq!(reply.status, status);
            assert_eq!(reply.allow_header(), None);
            assert_eq!(reply.authenticate_bearer, status == 401);
        }
    }
}
