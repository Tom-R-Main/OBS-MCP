// SPDX-License-Identifier: GPL-2.0-only
//! Native domain owners shared by the executable and consumer contract tests.
mod identity;

pub mod apply_scene;
pub mod apply_scene_transport;
pub mod apply_scene_workflow;
pub mod capture_backend;
pub mod capture_window;
pub mod clock;
pub mod config;
pub mod control;
pub mod custody;
pub mod custom_mutations;
pub mod demo;
pub mod demo_transport;
pub mod dynamic_visibility;
pub mod generic_requests;
pub mod http_ingress;
pub mod http_protocol;
pub mod js_json;
pub mod media_backend;
pub mod media_tools;
pub mod media_verification;
pub mod obs;
pub mod observation;
pub mod operation_store;
pub mod ordered_transform;
pub mod output_events;
pub mod output_start;
pub mod output_start_v2;
pub mod output_stop_result;
pub mod output_transport;
pub mod preflight;
pub mod preparation_journal;
pub mod production_observation;
pub mod property_list;
pub mod receipt_broker;
pub mod record_clip;
pub mod record_impact;
pub mod record_stop_transport;
pub mod resource_events;
pub mod restore_transport;
pub mod restore_workflow;
pub mod runtime_identity;
pub mod scene_audit;
pub mod scene_item_transform_mutation;
pub mod scene_plan;
pub mod service;
pub mod service_events;
pub mod service_read_tools;
pub mod service_read_workflows;
pub mod session;
pub mod session_observation;
pub mod snapshot_tools;
pub mod snapshots;
pub mod stage;
pub mod stage_transport;
pub mod take_monitor;
pub mod take_transport;
pub mod takes;
pub mod tool_validation;
pub mod trim;

pub(crate) mod capture_window_workflow;

pub mod advanced_commands;
pub mod native_profile;
pub mod opaque_commands;

pub mod network_transport;
#[cfg(all(
    feature = "platform-tls-evaluation",
    any(target_os = "macos", target_os = "linux")
))]
pub mod platform_transport;

#[cfg(all(
    feature = "platform-tls-evaluation",
    not(any(target_os = "macos", target_os = "linux"))
))]
compile_error!(
    "platform-tls-evaluation is supported only on macOS and Linux; disable this feature on other targets"
);
