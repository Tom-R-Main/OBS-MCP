// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::control::{
    CallerContext, ClaimError, ClaimOutcome, ConfirmationAnswer, ConfirmationContext,
    ControlDomain, Disposition, GateRequest, RefusalKind, ReleaseOutcome, TakeLock, UNIDENTIFIED,
    caller_name, client_can_elicit, live_confirmation_enabled, live_question, locked_action,
};
use serde_json::{Value, json};

fn request<'a>(tool: &'a str, arguments: &'a Value, caller: &'a str) -> GateRequest<'a> {
    GateRequest {
        tool_name: tool,
        arguments,
        read_only_hint: false,
        caller,
        confirmation: ConfirmationContext::default(),
    }
}

#[test]
fn identity_precedence_is_http_then_modern_then_legacy_then_unidentified() {
    let envelope = json!({"io.modelcontextprotocol/clientInfo":{"name":"modern"}});
    let declared = json!("http");
    let context = || CallerContext {
        http_client_id: Some(&declared),
        envelope: Some(&envelope),
        legacy_name: Some("legacy"),
    };
    assert_eq!(caller_name(context()), "http");
    assert_eq!(
        caller_name(CallerContext {
            http_client_id: None,
            ..context()
        }),
        "modern"
    );
    assert_eq!(
        caller_name(CallerContext {
            http_client_id: None,
            envelope: None,
            ..context()
        }),
        "legacy"
    );
    assert_eq!(caller_name(CallerContext::default()), UNIDENTIFIED);
    assert_eq!(
        caller_name(CallerContext {
            legacy_name: Some(""),
            ..Default::default()
        }),
        UNIDENTIFIED
    );
    let invalid = json!(7);
    assert_eq!(
        caller_name(CallerContext {
            http_client_id: Some(&invalid),
            ..context()
        }),
        "modern"
    );
    let whitespace = json!(" ");
    assert_eq!(
        caller_name(CallerContext {
            http_client_id: Some(&whitespace),
            ..context()
        }),
        " "
    );
}

#[test]
fn renewing_preserves_since_replaces_reason_and_extends_from_now() {
    let mut domain = ControlDomain::default();
    let ClaimOutcome::Claimed(first) = domain.claim("codex", 1, "first", 1000).unwrap() else {
        panic!("expected claim")
    };
    assert_eq!((first.since, first.expires_at), (1000, 61_000));
    let ClaimOutcome::Claimed(renewed) = domain.claim("codex", 2, "second", 30_000).unwrap() else {
        panic!("expected renewal")
    };
    assert_eq!((renewed.since, renewed.expires_at), (1000, 150_000));
    assert_eq!(renewed.reason, "second");
    assert_eq!(
        domain.claim("other", 1, "", 31_000),
        Ok(ClaimOutcome::HeldBy(renewed.clone()))
    );
    assert_eq!(
        domain.release("other", 31_000),
        ReleaseOutcome::HeldBy(renewed)
    );
    assert_eq!(domain.release("codex", 31_000), ReleaseOutcome::Released);
    assert_eq!(domain.release("codex", 31_000), ReleaseOutcome::NoLease);
}

#[test]
fn expiry_at_the_exact_boundary_releases_control_and_resets_since() {
    let mut domain = ControlDomain::default();
    domain.claim("codex", 1, "", 500).unwrap();
    assert!(domain.active_lease(60_499).is_some());
    assert!(domain.active_lease(60_500).is_none());
    let ClaimOutcome::Claimed(lease) = domain.claim("other", 1, "", 60_500).unwrap() else {
        panic!("expected fresh claim")
    };
    assert_eq!(lease.since, 60_500);
}

#[test]
fn unidentified_clients_cannot_claim_but_can_mutate_without_a_lease() {
    let mut domain = ControlDomain::default();
    assert_eq!(
        domain.claim(UNIDENTIFIED, 30, "", 0),
        Ok(ClaimOutcome::Unidentified)
    );
    assert_eq!(
        domain.evaluate(
            request("obs-set-current-scene", &json!({}), UNIDENTIFIED),
            0
        ),
        Disposition::Allow
    );
    domain.claim("codex", 1, "", 0).unwrap();
    assert!(matches!(
        domain.evaluate(
            request("obs-set-current-scene", &json!({}), UNIDENTIFIED),
            0
        ),
        Disposition::Refused {
            kind: RefusalKind::Lease,
            ..
        }
    ));
}

#[test]
fn duration_codepoint_reason_bounds_and_overflow_do_not_change_an_existing_lease() {
    let mut domain = ControlDomain::default();
    let claimed = domain.claim("codex", 1, "original", 0).unwrap();
    assert_eq!(
        domain.claim("codex", 0, "", 1),
        Err(ClaimError::InvalidMinutes)
    );
    assert_eq!(
        domain.claim("codex", 241, "", 1),
        Err(ClaimError::InvalidMinutes)
    );
    assert_eq!(
        domain.claim("codex", 1, &"😀".repeat(201), 1),
        Err(ClaimError::ReasonTooLong)
    );
    assert_eq!(
        domain.claim("other", 1, "", 1),
        match claimed {
            ClaimOutcome::Claimed(lease) => Ok(ClaimOutcome::HeldBy(lease)),
            _ => unreachable!(),
        }
    );
    let mut empty = ControlDomain::default();
    assert_eq!(
        empty.claim("codex", 1, "", u64::MAX),
        Err(ClaimError::ClockOverflow)
    );
    assert!(empty.active_lease(0).is_none());
}

#[test]
fn all_established_stop_and_control_management_exceptions_survive_a_competing_lease() {
    let mut domain = ControlDomain::default();
    domain.claim("holder", 1, "", 0).unwrap();
    domain.set_take_lock(Some(TakeLock {
        id: "take1".into(),
        locked: true,
    }));
    for tool in [
        "obs-claim-control",
        "obs-release-control",
        "obs-stop-record",
        "obs-stop-stream",
        "obs-take-stop",
        "obs-stop-virtual-cam",
        "obs-stop-replay-buffer",
        "obs-stop-output",
    ] {
        assert_eq!(
            domain.evaluate(request(tool, &json!({}), "other"), 1),
            Disposition::Allow,
            "{tool}"
        );
    }
    let mut read = request("obs-get-version", &Value::Null, "other");
    read.read_only_hint = true;
    assert_eq!(domain.evaluate(read, 1), Disposition::Allow);
    // The generic request tool remains a mutation surface even for a stop or a read.
    for kind in ["StopRecord", "GetVersion"] {
        assert!(matches!(
            domain.evaluate(
                request("obs-call-request", &json!({"requestType":kind}), "other"),
                1
            ),
            Disposition::Refused {
                kind: RefusalKind::Lease,
                ..
            }
        ));
    }
}

#[test]
fn lease_and_take_lock_refuse_before_any_confirmation_question() {
    let mut domain = ControlDomain::default();
    domain.claim("holder", 1, "", 0).unwrap();
    domain.set_take_lock(Some(TakeLock {
        id: "take1".into(),
        locked: true,
    }));
    let args =
        json!({"requests":[{"requestType":"SetVideoSettings"},{"requestType":"StartStream"}]});
    let check = |caller| GateRequest {
        confirmation: ConfirmationContext {
            enabled: true,
            can_elicit: true,
            input_responses: None,
        },
        ..request("obs-batch", &args, caller)
    };
    assert!(matches!(
        domain.evaluate(check("other"), 1),
        Disposition::Refused {
            kind: RefusalKind::Lease,
            ..
        }
    ));
    assert!(matches!(
        domain.evaluate(check("holder"), 1),
        Disposition::Refused {
            kind: RefusalKind::TakeLock,
            ..
        }
    ));
    domain.set_take_lock(Some(TakeLock {
        id: "take1".into(),
        locked: false,
    }));
    assert_eq!(
        domain.evaluate(check("holder"), 1),
        Disposition::InputRequired {
            question: "Start the live stream?".into()
        }
    );
    // A prior Allow does not confer permission after the owner changes.
    domain.release("holder", 1);
    domain.claim("other", 1, "", 1).unwrap();
    assert!(matches!(
        domain.evaluate(check("holder"), 1),
        Disposition::Refused {
            kind: RefusalKind::Lease,
            ..
        }
    ));
}

#[test]
fn take_lock_covers_named_raw_and_batched_settings_and_preserves_request_order() {
    for (tool, action) in [
        ("obs-set-current-profile", "switch profiles"),
        ("obs-create-profile", "create and switch to a profile"),
        ("obs-remove-profile", "remove a profile"),
        (
            "obs-set-current-scene-collection",
            "switch scene collections",
        ),
        (
            "obs-create-scene-collection",
            "create and switch to a scene collection",
        ),
        ("obs-set-profile-parameter", "change profile settings"),
        ("obs-set-record-directory", "change the recording directory"),
        ("obs-set-video-settings", "change the canvas and frame rate"),
        (
            "obs-set-stream-service-settings",
            "change the stream service",
        ),
        ("obs-set-output-settings", "change an output's settings"),
    ] {
        assert_eq!(locked_action(tool, &json!({})).as_deref(), Some(action));
    }
    assert_eq!(
        locked_action(
            "obs-call-request",
            &json!({"requestType":"SetOutputSettings"})
        )
        .as_deref(),
        Some("send SetOutputSettings")
    );
    assert_eq!(locked_action("obs-batch", &json!({"requests":[{"requestType":"GetVersion"},{"requestType":"SetCurrentProfile"},null,{"requestType":"SetVideoSettings"},{"requestType":"SetCurrentProfile"}]})).as_deref(), Some("send SetCurrentProfile, SetVideoSettings, SetCurrentProfile"));
    assert_eq!(locked_action("obs-stop-output", &json!({})), None);
    assert_eq!(locked_action("obs-call-request", &Value::Null), None);
}

#[test]
fn questions_cover_fixed_and_generic_live_requests_with_ordered_deduplication() {
    let args = json!({"requests":[{"requestType":"SetInputMute"},{"requestType":"StopRecord"},{"requestType":"StartStream"},{"requestType":"StopRecord"},{"requestType":"StartOutput","requestData":{"outputName":"simple_stream"}}]});
    assert_eq!(
        live_question("obs-batch", &args).as_deref(),
        Some(
            "Stop the recording? Start the live stream? Start the output simple_stream? This may start a live broadcast or recording."
        )
    );
    assert_eq!(
        live_question("obs-take-scene", &json!({})).as_deref(),
        Some("Transition the staged preview scene to program?")
    );
    assert_eq!(
        live_question(
            "obs-call-request",
            &json!({"requestType":"ToggleOutput","requestData":{"outputName":"adv_file_output"}})
        )
        .as_deref(),
        Some(
            "Toggle the output adv_file_output? If it is the stream or the recording, this can end it."
        )
    );
    assert_eq!(
        live_question("obs-stop-output", &json!({"outputName":"simple_stream"})).as_deref(),
        Some("Stop the output simple_stream? If it is the stream or the recording, this ends it.")
    );
    assert_eq!(
        live_question(
            "obs-batch",
            &json!({"requests":[{"requestType":"GetVersion"}]})
        ),
        None
    );
}

#[test]
fn live_question_json_coercion_retains_missing_null_arrays_and_numbers() {
    for (data, name) in [
        (json!({}), "undefined"),
        (json!({"outputName":null}), "null"),
        (json!({"outputName":[1,null,"x"]}), "1,,x"),
        (json!({"outputName":{}}), "[object Object]"),
        (json!({"outputName":1e21}), "1e+21"),
        (json!({"outputName":1e-7}), "1e-7"),
    ] {
        assert_eq!(
            live_question("obs-start-output", &data),
            Some(format!(
                "Start the output {name}? This may start a live broadcast or recording."
            ))
        );
    }
}

#[test]
fn modern_capabilities_override_legacy_even_when_empty_and_null_members_count_as_present() {
    let legacy = json!({"elicitation":{"form":{}}});
    assert!(client_can_elicit(None, Some(&legacy)));
    assert!(!client_can_elicit(
        Some(&json!({"io.modelcontextprotocol/clientCapabilities":{}})),
        Some(&legacy)
    ));
    assert!(client_can_elicit(
        Some(&json!({"io.modelcontextprotocol/clientCapabilities":{"elicitation":null}})),
        None
    ));
    assert!(client_can_elicit(
        Some(&json!({"io.modelcontextprotocol/clientCapabilities":null})),
        Some(&legacy)
    ));
    assert!(!client_can_elicit(None, None));
}

#[test]
fn elicitation_capable_clients_cannot_be_approved_by_the_model_confirm_argument() {
    let mut domain = ControlDomain::default();
    let args = json!({"confirm":true});
    let decision = domain.evaluate(
        GateRequest {
            confirmation: ConfirmationContext {
                enabled: true,
                can_elicit: true,
                input_responses: None,
            },
            ..request("obs-stop-stream", &args, "codex")
        },
        0,
    );
    assert_eq!(
        decision,
        Disposition::InputRequired {
            question: "Stop the live stream?".into()
        }
    );
    assert_eq!(
        decision.tool_result().unwrap(),
        json!({"resultType":"input_required","inputRequests":{"confirm":{"method":"elicitation/create","params":{"mode":"form","message":"Stop the live stream?","requestedSchema":{"type":"object","properties":{"confirm":{"type":"boolean","title":"Confirm","description":"Stop the live stream?"}},"required":["confirm"]}}}}})
    );
}

#[test]
fn established_bare_input_responses_accept_boolean_only_and_cancel_other_present_answers() {
    for (responses, expected) in [
        (
            json!({"confirm":{"action":"accept","content":{"confirm":true,"extra":"ignored"}}}),
            ConfirmationAnswer::Accepted(true),
        ),
        (
            json!({"confirm":{"action":"accept","content":{"confirm":false}}}),
            ConfirmationAnswer::Accepted(false),
        ),
        (
            json!({"confirm":{"action":"accept","content":{"confirm":"true"}}}),
            ConfirmationAnswer::OtherResponse,
        ),
        (
            json!({"confirm":{"action":"accept"}}),
            ConfirmationAnswer::OtherResponse,
        ),
        (
            json!({"confirm":{"action":"decline"}}),
            ConfirmationAnswer::OtherResponse,
        ),
        (
            json!({"confirm":{"action":"cancel"}}),
            ConfirmationAnswer::OtherResponse,
        ),
        (
            json!({"confirm":{"roots":[]}}),
            ConfirmationAnswer::OtherResponse,
        ),
        (
            json!({"confirm":{"role":"assistant","content":[]}}),
            ConfirmationAnswer::OtherResponse,
        ),
        (
            json!({"confirm":{"result":{"action":"accept","content":{"confirm":true}}}}),
            ConfirmationAnswer::Missing,
        ),
        (json!({"confirm":null}), ConfirmationAnswer::Missing),
        (json!({"confirm":[]}), ConfirmationAnswer::Missing),
    ] {
        assert_eq!(
            ConfirmationAnswer::from_responses(Some(&responses)),
            expected
        );
        let mut domain = ControlDomain::default();
        let args = json!({});
        let disposition = domain.evaluate(
            GateRequest {
                confirmation: ConfirmationContext {
                    enabled: true,
                    can_elicit: true,
                    input_responses: Some(&responses),
                },
                ..request("obs-stop-record", &args, "codex")
            },
            0,
        );
        assert_eq!(
            disposition,
            match expected {
                ConfirmationAnswer::Accepted(true) => Disposition::Allow,
                ConfirmationAnswer::Missing => Disposition::InputRequired {
                    question: "Stop the recording?".into()
                },
                _ => Disposition::Cancelled,
            }
        );
    }
}

#[test]
fn clients_without_elicitation_require_explicit_confirmation_and_declines_override_it() {
    let mut domain = ControlDomain::default();
    let no = json!({});
    let yes = json!({"confirm":true});
    let enabled = ConfirmationContext {
        enabled: true,
        ..Default::default()
    };
    assert!(matches!(
        domain.evaluate(
            GateRequest {
                confirmation: enabled,
                ..request("obs-start-stream", &no, "codex")
            },
            0
        ),
        Disposition::Refused {
            kind: RefusalKind::Confirmation,
            ..
        }
    ));
    assert_eq!(
        domain.evaluate(
            GateRequest {
                confirmation: enabled,
                ..request("obs-start-stream", &yes, "codex")
            },
            0
        ),
        Disposition::Allow
    );
    let decline = json!({"confirm":{"action":"decline"}});
    assert_eq!(
        domain.evaluate(
            GateRequest {
                confirmation: ConfirmationContext {
                    input_responses: Some(&decline),
                    ..enabled
                },
                ..request("obs-start-stream", &yes, "codex")
            },
            0
        ),
        Disposition::Cancelled
    );
    assert_eq!(
        Disposition::Cancelled.tool_result().unwrap(),
        json!({"content":[{"type":"text","text":"Cancelled; nothing was changed"}]})
    );
}

#[test]
fn claim_release_and_status_projection_matches_existing_public_messages() {
    let mut domain = ControlDomain::default();
    let result = domain.claim("codex", 1, "demo", 0).unwrap().tool_result();
    assert_eq!(
        result["content"][0]["text"],
        "codex has control (demo) until 00:01 UTC. Release it with obs-release-control"
    );
    assert_eq!(
        result["structuredContent"],
        json!({"holder":"codex","reason":"demo","since":0,"expiresAt":60_000})
    );
    assert_eq!(
        domain.status_result("codex", 1)["content"][0]["text"],
        "codex has control (demo) until 00:01 UTC (this client)"
    );
    assert_eq!(
        domain.release("codex", 1).tool_result(),
        json!({"content":[{"type":"text","text":"Released control of OBS"}]})
    );
    assert_eq!(
        domain.status_result("other", 1)["structuredContent"],
        json!({"caller":"other","lease":null})
    );
    assert!(!live_confirmation_enabled(None));
    assert!(live_confirmation_enabled(Some(" YES ")));
    assert!(!live_confirmation_enabled(Some("on")));
}
