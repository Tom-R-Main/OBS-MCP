// SPDX-License-Identifier: GPL-2.0-only
//! The pinned SDK is a development oracle, never part of the native control owner.
use obs_mcp_native::control::{ConfirmationAnswer, Disposition};
use serde_json::Value;
use std::{path::Path, process::Command};

#[test]
fn confirmation_projection_and_admission_match_the_actual_pinned_sdk() {
    let oracle = Command::new("node")
        .args(["--input-type=module", "-e", r#"
import {inputRequired,inputResponse,acceptedContent} from '@modelcontextprotocol/server';
import {z} from 'zod';
const variants=[null,{},
 {confirm:{action:'accept',content:{confirm:true}}},
 {confirm:{action:'accept',content:{confirm:false}}},
 {confirm:{action:'accept',content:{confirm:'yes'}}},
 {confirm:{action:'decline'}},{confirm:{action:'cancel'}},
 {confirm:{action:'invalid'}},{confirm:{roots:[]}},
 {confirm:{role:'assistant',content:{type:'text',text:'hi'}}},
 {confirm:[]},{confirm:null}];
const schema=z.object({confirm:z.boolean()});
const question='Stop the recording?';
console.log(JSON.stringify({required:inputRequired({inputRequests:{confirm:inputRequired.elicit({
 message:question,requestedSchema:{type:'object',properties:{confirm:{type:'boolean',title:'Confirm',description:question}},required:['confirm']}
})}}),answers:variants.map(input=>({input,kind:inputResponse(input,'confirm').kind,accepted:acceptedContent(input,'confirm',schema)??null}))}));
"#])
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap())
        .output()
        .expect("the migration's SDK oracle requires the project development Node runtime");
    assert!(
        oracle.status.success(),
        "SDK oracle failed: {}",
        String::from_utf8_lossy(&oracle.stderr)
    );
    let oracle: Value = serde_json::from_slice(&oracle.stdout).unwrap();
    assert_eq!(
        Disposition::InputRequired {
            question: "Stop the recording?".into()
        }
        .tool_result(),
        Some(oracle["required"].clone())
    );
    for answer in oracle["answers"].as_array().unwrap() {
        let expected = if let Some(confirmed) = answer["accepted"]["confirm"].as_bool() {
            ConfirmationAnswer::Accepted(confirmed)
        } else if answer["kind"] == "missing" {
            ConfirmationAnswer::Missing
        } else {
            ConfirmationAnswer::OtherResponse
        };
        let input = (!answer["input"].is_null()).then_some(&answer["input"]);
        assert_eq!(ConfirmationAnswer::from_responses(input), expected);
    }
}
