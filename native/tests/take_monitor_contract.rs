// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::take_monitor::{
    CleanupError, MonitorOptions, Phase, SampleKind, SampleObservation, StillTracker,
    TakeMonitorCore,
};
use serde_json::{Value, json};
use std::{path::Path, process::Command};

const START: i64 = 1_793_448_000_000;
const UTC: &str = "2026-10-31T12:00:00.000Z";
fn core(options: MonitorOptions) -> TakeMonitorCore {
    TakeMonitorCore::new(START, UTC.into(), options)
}
fn oracle(input: &Value) -> Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let output = Command::new("node")
        .arg("--input-type=module")
        .arg("-e")
        .arg(r#"
import {EventEmitter} from 'node:events';
import {TakeMonitor} from './build/tools/take-monitor.js';
import {encodePpm} from './build/tools/frame-sample.js';
const input=JSON.parse(process.env.TAKE_MONITOR_TEST_INPUT);
let now=input.start, nextStats=input.initialStats??{}, frame=[],pendingPicture,releasePicture;
Date.now=()=>now;
globalThis.setInterval=()=>({unref(){}});
globalThis.clearInterval=()=>{};
let subscriptions=0,releases=0;
class Client extends EventEmitter {
  async sendRequest(type,data){
    if(type==='GetStats')return structuredClone(nextStats);
    if(type==='GetProfileParameter') {
      const names={Mode:'Advanced',RecType:'Standard',RecTracks:String((input.recordTracks??[1]).reduce((mask,track)=>mask|(1<<(track-1)),0))};
      return {parameterValue:names[data.parameterName]??null};
    }
    if(type==='GetInputList') {
      if(input.tracksUnknown) throw Error('tracks fixture unavailable');
      return {inputs:Object.keys(input.inputTracks??{}).map(inputName=>({inputName}))};
    }
    if(type==='GetCurrentProgramScene')return input.program??{currentProgramSceneName:'Program'};
    if(type==='GetSourceScreenshot'){const image={imageData:'data:image/x-portable-pixmap;base64,'+encodePpm(frame.length,1,i=>[frame[i],frame[i],frame[i]]).toString('base64')};if(input.deferPictures)await new Promise(resolve=>releasePicture=resolve);return image;}
    throw Error('Unexpected fixture request '+type);
  }
  async sendBatch(requests){return requests.map(r=>({ok:true,responseData:{inputAudioTracks:input.inputTracks[r.requestData.inputName]}}));}
  subscribeHighVolume(){subscriptions++;return ()=>{releases++;};}
  getConnectionStatus(){return {versionInfo:{supportedImageFormats:input.ppm===false?['png']:['ppm']}};}
}
const client=new Client();const monitor=new TakeMonitor(client,input.options??{});
const snapshots=[];
try {
  await monitor.start();
  for(const op of input.ops){
    now=input.start+op.at;
    if(op.kind==='event')client.emit(op.event,op.data);
    else if(op.kind==='disconnect')client.emit('disconnected');
    else if(op.kind==='stats'){nextStats=op.data;await monitor.sampleStats().catch(()=>{});}
    else if(op.kind==='picture'){frame=op.luma;await monitor.samplePicture();}
    else if(op.kind==='beginPicture'){frame=op.luma;pendingPicture=monitor.samplePicture();}
    else if(op.kind==='resolvePicture'){releasePicture();await pendingPicture;}
    else if(op.kind==='mark')monitor.mark(op.name);
    else if(op.kind==='expectStop')monitor.expectStop();
    else if(op.kind==='snapshot')snapshots.push(monitor.summary());
    else if(op.kind==='stop'){if(op.finalStats!==undefined)nextStats=op.finalStats;await monitor.stop();}
  }
  console.log(JSON.stringify({snapshots,outputPath:monitor.outputPath()}));
} finally {
  await monitor.stop();
  if(subscriptions!==1||releases!==1||client.eventNames().length!==0)throw Error('Oracle cleanup did not release every monitor-owned observer');
}
"#)
        .current_dir(root)
        .env("TAKE_MONITOR_TEST_INPUT", input.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "actual TypeScript monitor oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn options(input: &Value) -> MonitorOptions {
    let mut options = MonitorOptions::default();
    if let Some(v) = input.get("expectSilent").and_then(Value::as_bool) {
        options.expect_silent = v;
    }
    if let Some(v) = input.get("silenceThresholdDb").and_then(Value::as_f64) {
        options.silence_threshold_db = v;
    }
    if let Some(v) = input.get("sampleIntervalMs").and_then(Value::as_f64) {
        options.sample_interval_ms = v;
    }
    if let Some(v) = input.get("statsIntervalMs").and_then(Value::as_f64) {
        options.stats_interval_ms = v;
    }
    if let Some(v) = input.get("stillAfterSeconds").and_then(Value::as_f64) {
        options.still_after_seconds = v;
    }
    options
}
fn native(input: &Value) -> Value {
    let mut monitor = core(options(&input["options"]));
    let mut stats = input.get("initialStats").cloned().unwrap_or(json!({}));
    if input["tracksUnknown"] == true {
        monitor.tracks_unknown(START, "tracks fixture unavailable");
    } else {
        let tracks = input
            .get("inputTracks")
            .and_then(Value::as_object)
            .map(|tracks| {
                tracks
                    .iter()
                    .map(|(name, value)| (name.clone(), value.as_object().unwrap().clone()))
                    .collect()
            })
            .unwrap_or_default();
        let recorded = input
            .get("recordTracks")
            .and_then(Value::as_array)
            .map(|tracks| {
                tracks
                    .iter()
                    .map(|track| track.as_u64().unwrap() as u8)
                    .collect()
            })
            .unwrap_or(vec![1]);
        monitor.load_tracks(recorded, tracks);
    }
    monitor.load_program_scene(
        input
            .get("program")
            .unwrap_or(&json!({"currentProgramSceneName":"Program"})),
    );
    monitor.observe_stats(START, &stats);
    assert!(monitor.complete_setup(START, input["ppm"] != false));
    let mut snapshots = vec![];
    let mut pending_picture = None;
    for op in input["ops"].as_array().unwrap() {
        let now = START + op["at"].as_i64().unwrap();
        match op["kind"].as_str().unwrap() {
            "event" => monitor.on_event(now, op["event"].as_str().unwrap(), &op["data"]),
            "disconnect" => monitor.on_disconnect(now),
            "stats" => {
                stats = op["data"].clone();
                monitor.observe_stats(now, &stats);
            }
            "picture" => {
                if let Some(ticket) = monitor.begin_sample(SampleKind::Picture) {
                    let luma: Vec<u8> = op["luma"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|v| v.as_u64().unwrap() as u8)
                        .collect();
                    monitor.finish_sample(ticket, now, SampleObservation::Picture(&luma));
                }
            }
            "beginPicture" => pending_picture = monitor.begin_sample(SampleKind::Picture),
            "resolvePicture" => {
                if let Some(ticket) = pending_picture.take() {
                    let luma = op["luma"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|v| v.as_u64().unwrap() as u8)
                        .collect::<Vec<_>>();
                    monitor.finish_sample(ticket, now, SampleObservation::Picture(&luma));
                }
            }
            "mark" => monitor.mark(now, op["name"].as_str().unwrap().into()),
            "expectStop" => monitor.expect_stop(),
            "snapshot" => snapshots.push(serde_json::to_value(monitor.summary(now)).unwrap()),
            "stop" => {
                if monitor.begin_stop(now) {
                    if let Some(final_stats) = op.get("finalStats") {
                        stats = final_stats.clone();
                    }
                    monitor.observe_stats(now, &stats);
                    monitor.finish_cleanup().unwrap();
                }
            }
            _ => panic!("unknown fixture operation"),
        }
    }
    json!({"snapshots":snapshots,"outputPath":monitor.output_path()})
}
fn compare(mut fixture: Value) -> Value {
    fixture["start"] = json!(START);
    let expected = oracle(&fixture);
    let actual = native(&fixture);
    assert_same("$", &actual, &expected);
    actual
}
// JSON has one numeric type. serde_json separately represents integer and
// float spellings (0 vs 0.0); compare their exact f64 value, without eliding
// timestamps, counters, warnings, or any other report field.
fn assert_same(path: &str, actual: &Value, expected: &Value) {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => assert_eq!(a.as_f64(), b.as_f64(), "{path}"),
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len(), "{path} length");
            for (index, (a, b)) in a.iter().zip(b).enumerate() {
                assert_same(&format!("{path}[{index}]"), a, b);
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            assert_eq!(
                a.keys().collect::<Vec<_>>(),
                b.keys().collect::<Vec<_>>(),
                "{path} keys"
            );
            for (key, a) in a {
                assert_same(&format!("{path}.{key}"), a, &b[key]);
            }
        }
        _ => assert_eq!(actual, expected, "{path}"),
    }
}
fn event(at: i64, name: &str, data: Value) -> Value {
    json!({"kind":"event","at":at,"event":name,"data":data})
}
fn picture(at: i64, luma: &[u8]) -> Value {
    json!({"kind":"picture","at":at,"luma":luma})
}
fn snapshot(at: i64) -> Value {
    json!({"kind":"snapshot","at":at})
}
fn stats(at: i64, data: Value) -> Value {
    json!({"kind":"stats","at":at,"data":data})
}

#[test]
fn audio_tracks_unknown_coercion_peak_retention_and_audibility_transitions_match_typescript() {
    let result = compare(
        json!({"options":{"expectSilent":true},"recordTracks":[2],"inputTracks":{"unrecorded":{"1":true},"recorded":{"2":true}},"ops":[
            event(0,"InputVolumeMeters",json!({"inputs":[null,{}, {"inputName":"ignored","inputLevelsMul":null},{"inputName":"unknown","inputLevelsMul":[[0,"0.1","0.2"],null,[0,-1,"bad"]]},{"inputName":"recorded","inputLevelsMul":[[0,0.001,0.01]]},{"inputName":"unrecorded","inputLevelsMul":[[0,1,1]]}]})),
            snapshot(1),
            event(10,"InputAudioTracksChanged",json!({"inputName":"unknown","inputAudioTracks":{"1":true}})),
            event(20,"InputVolumeMeters",json!({"inputs":[{"inputName":"unknown","inputLevelsMul":[[0,0.5,0.9]]},{"inputName":"recorded","inputLevelsMul":[[0,0.1,0.1]]}]})),
            event(30,"InputVolumeMeters",json!({"inputs":[{"inputName":"recorded","inputLevelsMul":[[0,0,0]]},{"inputName":"silent","inputLevelsMul":[]}]})),
            snapshot(40),json!({"kind":"stop","at":1000}),snapshot(2000)
        ]}),
    );
    assert_eq!(
        result["snapshots"][0]["warnings"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        result["snapshots"][1]["audio"][0]["inputName"],
        "unrecorded"
    );
    let final_summary = &result["snapshots"][2];
    assert_eq!(final_summary["audio"][1]["peakDb"], json!(-6.0));
    assert_eq!(final_summary["audio"][3]["peakDb"], Value::Null);
}

#[test]
fn missing_stats_reset_nan_throttle_and_unimplemented_cpu_checks_match_typescript() {
    let result = compare(
        json!({"initialStats":{"renderSkippedFrames":10,"renderTotalFrames":100,"outputSkippedFrames":5,"outputTotalFrames":90},"ops":[
            stats(0,json!({"renderSkippedFrames":12,"renderTotalFrames":160,"outputSkippedFrames":7,"outputTotalFrames":150,"cpuUsage":100,"activeFps":0})),
            stats(9999,json!({"renderSkippedFrames":15,"renderTotalFrames":180,"outputSkippedFrames":8,"outputTotalFrames":160})),
            stats(10000,json!({"renderSkippedFrames":16,"renderTotalFrames":190,"outputSkippedFrames":9,"outputTotalFrames":170})),
            stats(11000,json!({})),
            stats(12000,json!({"renderSkippedFrames":"2","renderTotalFrames":true,"outputSkippedFrames":null,"outputTotalFrames":[]})),
            stats(20000,json!({"renderSkippedFrames":{},"renderTotalFrames":"unknown","outputSkippedFrames":["4"],"outputTotalFrames":"0x20"})),
            snapshot(20001),json!({"kind":"stop","at":22000,"finalStats":{"renderSkippedFrames":3}}),snapshot(30000)
        ]}),
    );
    assert!(result["snapshots"][0]["frames"]["renderSkipped"].is_null());
    assert_eq!(
        result["snapshots"][0]["warnings"].as_array().unwrap().len(),
        6
    );
    assert_eq!(result["snapshots"][1]["durationSeconds"], 22.0);
}

#[test]
fn black_runs_still_median_noise_steps_and_scene_switch_match_typescript() {
    let mut ops = vec![];
    for (i, luma) in [
        vec![40, 40, 40],
        vec![40, 40, 40],
        vec![80, 80, 40],
        vec![40, 40, 40],
        vec![40, 40, 40],
        vec![40, 40, 40],
        vec![40, 40, 40],
        vec![100, 100, 40],
        vec![100, 100, 40],
        vec![100, 100, 40],
        vec![100, 100, 40],
    ]
    .iter()
    .enumerate()
    {
        ops.push(picture(i as i64 * 1000, luma));
    }
    ops.push(snapshot(11000));
    ops.push(event(
        11500,
        "CurrentProgramSceneChanged",
        json!({"sceneName":"Other"}),
    ));
    ops.extend([
        picture(12000, &[0, 0, 0]),
        picture(13000, &[0, 0, 0]),
        snapshot(14000),
        picture(14500, &[16, 16, 16]),
        snapshot(15000),
        json!({"kind":"stop","at":16000}),
        snapshot(17000),
    ]);
    let result = compare(json!({"ops":ops}));
    assert_eq!(
        result["snapshots"][0]["stills"],
        // Only four new-picture samples follow the step: its trailing
        // candidate has not reached an equal-median comparison yet.
        json!([{"startSeconds":0.0,"endSeconds":7.0}])
    );
    assert_eq!(
        result["snapshots"][1]["blanks"],
        json!([{"startSeconds":12.0,"endSeconds":14.0}])
    );
    assert_eq!(
        result["snapshots"][2]["blanks"],
        json!([{"startSeconds":12.0,"endSeconds":14.5}])
    );
}

#[test]
fn four_live_a_b_samples_never_claim_unchanged_before_a_median_comparison() {
    let result = compare(
        json!({"ops":[picture(1050,&[20,20,20]),picture(2050,&[20,20,20]),picture(3050,&[200,200,200]),picture(4050,&[200,200,200]),snapshot(4990),json!({"kind":"stop","at":4990}),snapshot(5000)]}),
    );
    for summary in result["snapshots"].as_array().unwrap() {
        assert_eq!(summary["framesSampled"], 4);
        assert_eq!(summary["pictureChecks"], "on");
        assert_eq!(
            summary["stills"],
            json!([]),
            "warmup is not unchanged-picture evidence"
        );
        assert_eq!(summary["warnings"], json!([]));
    }
}

#[test]
fn continuously_changing_six_sample_tail_is_not_qualified_unchanged_evidence() {
    let mut tracker = StillTracker::new(3.0);
    for at in 0..6 {
        tracker.push(at as f64, vec![20 + at * 20; 3]);
    }
    assert!(tracker.still_since().is_none());
    assert!(
        tracker.finish(6.0).is_empty(),
        "a comparison observing motion cannot qualify its moving tail as static"
    );
    let mut ops = (0..6)
        .map(|at| picture(at * 1000, &[20 + at as u8 * 20; 3]))
        .collect::<Vec<_>>();
    ops.extend([
        snapshot(6000),
        json!({"kind":"stop","at":6000}),
        snapshot(7000),
    ]);
    let result = compare(json!({"ops":ops}));
    for summary in result["snapshots"].as_array().unwrap() {
        assert_eq!(summary["stills"], json!([]));
    }
}

#[test]
fn stable_tail_requires_a_later_stable_comparison_and_full_frame_alternation_stays_unqualified() {
    let mut tracker = StillTracker::new(3.0);
    for at in 0..6 {
        tracker.push(at as f64, vec![20 + at * 20; 3]);
    }
    for at in 6..9 {
        tracker.push(at as f64, vec![120; 3]);
        assert!(tracker.still_since().is_none());
    }
    tracker.push(9.0, vec![120; 3]);
    assert_eq!(tracker.still_since(), Some(5.0));
    assert_eq!(
        tracker
            .finish(10.0)
            .iter()
            .map(|s| (s.start_seconds, s.end_seconds))
            .collect::<Vec<_>>(),
        vec![(5.0, 10.0)]
    );
    let mut tracker = StillTracker::new(3.0);
    for at in 0..12 {
        tracker.push(at as f64, vec![if at % 2 == 0 { 20 } else { 200 }; 3]);
    }
    assert!(tracker.finish(12.0).is_empty());
    let mut stable = (0..6)
        .map(|at| picture(at * 1000, &[20 + at as u8 * 20; 3]))
        .collect::<Vec<_>>();
    stable.extend((6..10).map(|at| picture(at * 1000, &[120; 3])));
    stable.extend([
        snapshot(10000),
        json!({"kind":"stop","at":10000}),
        snapshot(11000),
    ]);
    let result = compare(json!({"ops":stable}));
    for summary in result["snapshots"].as_array().unwrap() {
        assert_eq!(
            summary["stills"],
            json!([{"startSeconds":5.0,"endSeconds":10.0}])
        );
    }
    let mut alternating = (0..12)
        .map(|at| picture(at * 1000, &[if at % 2 == 0 { 20 } else { 200 }; 3]))
        .collect::<Vec<_>>();
    alternating.extend([
        snapshot(12000),
        json!({"kind":"stop","at":12000}),
        snapshot(13000),
    ]);
    let result = compare(json!({"ops":alternating}));
    for summary in result["snapshots"].as_array().unwrap() {
        assert_eq!(summary["stills"], json!([]));
    }
}

#[test]
fn first_complete_comparison_qualifies_stable_and_old_median_spans_but_dimension_reset_revokes_it()
{
    let mut tracker = StillTracker::new(3.0);
    for at in 0..5 {
        tracker.push(at as f64, vec![20; 3]);
        assert!(tracker.still_since().is_none());
    }
    tracker.push(5.0, vec![20; 3]);
    assert_eq!(tracker.still_since(), Some(0.0));
    tracker.push(6.0, vec![200; 4]);
    assert!(tracker.still_since().is_none());
    assert_eq!(
        tracker
            .spans()
            .iter()
            .map(|s| (s.start_seconds, s.end_seconds))
            .collect::<Vec<_>>(),
        vec![(0.0, 5.0)]
    );
    assert_eq!(
        tracker
            .finish(10.0)
            .iter()
            .map(|s| (s.start_seconds, s.end_seconds))
            .collect::<Vec<_>>(),
        vec![(0.0, 5.0)]
    );
    let mut tracker = StillTracker::new(3.0);
    for at in 0..6 {
        tracker.push(at as f64, vec![if at < 3 { 20 } else { 200 }; 3]);
    }
    assert_eq!(
        tracker
            .spans()
            .iter()
            .map(|s| (s.start_seconds, s.end_seconds))
            .collect::<Vec<_>>(),
        vec![(0.0, 3.0)]
    );
}

#[test]
fn pause_and_program_switch_start_new_stillness_warmup_without_changing_picture_capability() {
    for boundary in ["pause", "scene"] {
        let mut ops = (0..4)
            .map(|i| picture(i * 1000, &[20; 3]))
            .collect::<Vec<_>>();
        if boundary == "pause" {
            ops.extend([
                event(
                    3500,
                    "RecordStateChanged",
                    json!({"outputState":"OBS_WEBSOCKET_OUTPUT_PAUSED"}),
                ),
                event(
                    4500,
                    "RecordStateChanged",
                    json!({"outputState":"OBS_WEBSOCKET_OUTPUT_RESUMED"}),
                ),
            ]);
        } else {
            ops.push(event(
                3500,
                "CurrentProgramSceneChanged",
                json!({"sceneName":"Other"}),
            ));
        }
        ops.extend([
            picture(5000, &[20; 3]),
            picture(6000, &[20; 3]),
            snapshot(7000),
            json!({"kind":"stop","at":7000}),
            snapshot(8000),
        ]);
        let fixture = json!({"ops":ops});
        let result = native(&fixture);
        for summary in result["snapshots"].as_array().unwrap() {
            assert_eq!(
                summary["stills"],
                json!([]),
                "{boundary} must revoke warmup history"
            );
            assert_eq!(summary["pictureChecks"], "on");
        }
        compare(fixture);
    }
}

#[test]
fn in_flight_picture_cannot_repopulate_history_after_pause_or_program_switch() {
    for (name, data) in [
        (
            "RecordStateChanged",
            json!({"outputState":"OBS_WEBSOCKET_OUTPUT_PAUSED"}),
        ),
        ("CurrentProgramSceneChanged", json!({"sceneName":"Other"})),
    ] {
        let mut monitor = core(MonitorOptions::default());
        monitor.load_program_scene(&json!({"currentProgramSceneName":"Program"}));
        assert!(monitor.complete_setup(START, true));
        let ticket = monitor.begin_sample(SampleKind::Picture).unwrap();
        monitor.on_event(START + 1000, name, &data);
        assert!(!monitor.finish_sample(ticket, START + 2000, SampleObservation::Picture(&[20; 3])));
        assert_eq!(monitor.summary(START + 2000).frames_sampled, 0);
        compare(
            json!({"deferPictures":true,"ops":[json!({"kind":"beginPicture","at":0,"luma":[20,20,20]}),event(1000,name,data),json!({"kind":"resolvePicture","at":2000,"luma":[20,20,20]}),snapshot(2000),json!({"kind":"stop","at":2000}),snapshot(3000)]}),
        );
    }
}

#[test]
fn pauses_record_paths_unexpected_stops_and_backward_clock_match_typescript() {
    compare(json!({"options":{"expectSilent":true},"ops":[
        picture(5,&[0,0]),picture(1005,&[0,0]),
        event(1250,"RecordStateChanged",json!({"outputState":"OBS_WEBSOCKET_OUTPUT_PAUSED","outputPath":"/fixture/first.mkv"})),
        event(1500,"RecordStateChanged",json!({"outputState":"OBS_WEBSOCKET_OUTPUT_PAUSED"})),
        event(1700,"InputVolumeMeters",json!({"inputs":[{"inputName":"ignored","inputLevelsMul":[[0,1,1]]}]})),picture(1800,&[60,60]),snapshot(2000),
        event(2250,"RecordStateChanged",json!({"outputState":"OBS_WEBSOCKET_OUTPUT_RESUMED"})),
        event(2251,"RecordStateChanged",json!({"outputState":"OBS_WEBSOCKET_OUTPUT_RESUMED"})),
        event(2300,"RecordStateChanged",json!({"outputActive":false,"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED","outputPath":"/fixture/last.mkv"})),
        event(2400,"RecordFileChanged",json!({"newOutputPath":"/fixture/not-record-state.mkv"})),
        json!({"kind":"mark","at":2405,"name":"chapter"}),snapshot(2405),snapshot(-5),
        json!({"kind":"stop","at":2500}),snapshot(9999),
        event(11000,"RecordStateChanged",json!({"outputActive":false,"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"})),snapshot(11000)
    ]}));
}

#[test]
fn paused_stop_freezes_recorded_time_and_final_stats_match_typescript() {
    compare(
        json!({"ops":[picture(0,&[30,30]),picture(3000,&[30,30]),event(4000,"RecordStateChanged",json!({"outputState":"OBS_WEBSOCKET_OUTPUT_PAUSED"})),json!({"kind":"stop","at":9000,"finalStats":{"renderSkippedFrames":1,"renderTotalFrames":90}}),snapshot(20000),json!({"kind":"stop","at":30000}),snapshot(40000)]}),
    );
}

#[test]
fn logged_event_coercion_track_failure_and_disabled_picture_checks_match_typescript() {
    compare(json!({"tracksUnknown":true,"ppm":false,"ops":[
        event(1,"SceneItemEnableStateChanged",json!({})),event(2,"SceneItemEnableStateChanged",json!({"sceneName":null,"sceneItemId":[1,2],"sceneItemEnabled":{}})),
        event(3,"InputMuteStateChanged",json!({"inputName":null,"inputMuted":"no"})),event(4,"InputAudioTracksChanged",json!({"inputName":[] ,"inputAudioTracks":{}})),
        event(5,"CurrentProgramSceneChanged",json!({"sceneName":false})),event(6,"StreamStateChanged",json!({"outputState":null})),event(7,"RecordFileChanged",json!({"newOutputPath":{"x":1}})),
        event(8,"InputVolumeMeters",json!({"inputs":[{"inputName":"unknown tracks","inputLevelsMul":[[0,1,1]]}]})),json!({"kind":"expectStop","at":9}),
        event(10,"RecordStateChanged",json!({"outputActive":false,"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"})),json!({"kind":"disconnect","at":20}),snapshot(21)
    ]}));
}

#[test]
fn warning_and_event_caps_are_exact_and_independent_match_typescript() {
    let mut ops = vec![];
    for i in 0..550 {
        ops.push(event(
            i,
            "InputMuteStateChanged",
            json!({"inputName":"mic","inputMuted":i%2==0}),
        ));
    }
    for i in 0..25 {
        ops.push(json!({"kind":"disconnect","at":1000+i}));
        ops.push(event(
            2000 + i,
            "RecordStateChanged",
            json!({"outputActive":false,"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"}),
        ));
    }
    for i in 0..25 {
        ops.push(event(
            3000 + i,
            "InputVolumeMeters",
            json!({"inputs":[{"inputName":format!("mic{i}"),"inputLevelsMul":[[0,1,1]]}]}),
        ));
    }
    ops.push(snapshot(5000));
    let result = compare(json!({"options":{"expectSilent":true},"ops":ops}));
    assert_eq!(
        result["snapshots"][0]["events"].as_array().unwrap().len(),
        500
    );
    let warnings = result["snapshots"][0]["warnings"].as_array().unwrap();
    for kind in ["connection", "output", "audio"] {
        assert_eq!(
            warnings
                .iter()
                .filter(|warning| warning["kind"] == kind)
                .count(),
            20
        );
    }
}

#[test]
fn dimensions_thresholds_empty_frames_and_half_millisecond_rounding_match_typescript() {
    compare(
        json!({"options":{"sampleIntervalMs":250,"stillAfterSeconds":0.5},"ops":[snapshot(-5),snapshot(5),picture(0,&[]),picture(250,&[]),picture(500,&[15]),picture(750,&[16]),picture(1000,&[30,30]),picture(1250,&[42,42]),picture(1500,&[30,43]),picture(1750,&[30,30]),picture(2000,&[30,30]),picture(2250,&[30,30]),snapshot(2500),json!({"kind":"stop","at":3005}),snapshot(9999)]}),
    );
}

#[test]
fn null_and_falsey_stats_samples_reestablish_the_actual_source_baseline() {
    compare(json!({"initialStats":{"renderSkippedFrames":5},"ops":[
        stats(1000,Value::Null),stats(2000,json!({"renderSkippedFrames":10})),
        stats(3000,json!({"renderSkippedFrames":12})),stats(4000,json!(false)),
        stats(5000,json!({"renderSkippedFrames":100})),stats(6000,json!({"renderSkippedFrames":101})),
        stats(7000,json!({"renderSkippedFrames":"Infinity","outputSkippedFrames":"1e21"})),
        stats(20000,json!({"renderSkippedFrames":"Infinity","outputSkippedFrames":"1e22"})),
        snapshot(20000)
    ]}));
}

#[test]
fn deterministic_noisy_meter_picture_and_stats_history_matches_actual_typescript() {
    let mut seed = 0x584b_1293_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed >> 32
    };
    let mut ops = vec![];
    let mut time = 0;
    for index in 0..180 {
        time += i64::try_from(next() % 1500).unwrap() - 100;
        let luma = (0..8)
            .map(|pixel| {
                if pixel < 2 && index % 23 < 6 {
                    80
                } else {
                    u8::try_from(next() % 15 + 20).unwrap()
                }
            })
            .collect::<Vec<_>>();
        ops.push(picture(time, &luma));
        let levels = [
            json!(null),
            json!("0.001"),
            json!([0.01]),
            json!(true),
            json!("0x1"),
            json!(0),
            json!({}),
            json!("invalid"),
        ];
        ops.push(event(time,"InputVolumeMeters",json!({"inputs":[{"inputName":format!("input{}",index%3),"inputLevelsMul":[[0,levels[(next()%8)as usize],levels[(next()%8)as usize]],null]}]})));
        if index % 9 == 0 {
            ops.push(stats(time,json!({"renderSkippedFrames":next()%10,"outputSkippedFrames":next()%3,"renderTotalFrames":index*30,"outputTotalFrames":index*30})));
        }
        if index % 17 == 0 {
            ops.push(snapshot(time));
        }
    }
    ops.push(json!({"kind":"stop","at":time}));
    ops.push(snapshot(time + 50000));
    compare(json!({"options":{"expectSilent":true},"ops":ops}));
}

#[test]
fn actual_typescript_stop_during_pending_setup_has_no_late_subscriptions_or_timers() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let output = Command::new("node")
        .arg("--input-type=module")
        .arg("-e")
        .arg(
            r#"
import {EventEmitter} from 'node:events';
import {TakeMonitor} from './build/tools/take-monitor.js';
let now=1793448000000,releaseSetup;Date.now=()=>now;
const setup=new Promise(resolve=>releaseSetup=resolve);
let subscriptions=0,timers=0;
globalThis.setInterval=()=>{timers++;return {unref(){}}};globalThis.clearInterval=()=>{};
class Client extends EventEmitter {
  async sendRequest(type){
    if(type==='GetCurrentProgramScene'){await setup;return {currentProgramSceneName:'Program'};}
    if(type==='GetStats')return {};
    if(type==='GetProfileParameter')return {parameterValue:'Simple'};
    if(type==='GetInputList')return {inputs:[]};
    throw Error(type);
  }
  async sendBatch(){return []}
  subscribeHighVolume(){subscriptions++;return ()=>{}}
  getConnectionStatus(){return {versionInfo:{supportedImageFormats:['ppm']}}}
}
const client=new Client(),monitor=new TakeMonitor(client);
const starting=monitor.start();now+=500;
const a=monitor.stop(),b=monitor.stop();
if(a!==b)throw Error('Stop owner was duplicated');
await a;releaseSetup();await starting;
client.emit('RecordStateChanged',{outputPath:'late',outputState:'OBS_WEBSOCKET_OUTPUT_PAUSED'});
if(subscriptions||timers||client.eventNames().length)throw Error('Late setup rearmed monitor');
console.log(JSON.stringify({summary:monitor.summary(),outputPath:monitor.outputPath()}));
"#,
        )
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "TypeScript setup cleanup oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected: Value = serde_json::from_slice(&output.stdout).unwrap();
    let mut monitor = core(MonitorOptions::default());
    monitor.observe_stats(START, &json!({}));
    monitor.begin_stop(START + 500);
    monitor.observe_stats(START + 500, &json!({}));
    monitor.finish_cleanup().unwrap();
    assert!(!monitor.complete_setup(START + 500, true));
    monitor.on_event(
        START + 500,
        "RecordStateChanged",
        &json!({"outputPath":"late","outputState":"OBS_WEBSOCKET_OUTPUT_PAUSED"}),
    );
    assert_same(
        "$",
        &json!({"summary":monitor.summary(START+500),"outputPath":monitor.output_path()}),
        &expected,
    );
}

#[test]
fn sample_tickets_cancel_on_drop_serialize_each_kind_and_bind_to_owner() {
    let mut monitor = core(MonitorOptions::default());
    assert!(monitor.begin_sample(SampleKind::Stats).is_none());
    monitor.load_program_scene(&json!({"sceneName":"Program"}));
    monitor.complete_setup(START, true);
    let first = monitor.begin_sample(SampleKind::Stats).unwrap();
    assert!(monitor.begin_sample(SampleKind::Stats).is_none());
    let picture = monitor.begin_sample(SampleKind::Picture).unwrap();
    assert!(monitor.samples_pending());
    drop(first);
    let next = monitor.begin_sample(SampleKind::Stats).unwrap();
    assert!(monitor.finish_sample(next, START, SampleObservation::Stats(&json!({}))));
    let mut other = core(MonitorOptions::default());
    other.complete_setup(START, true);
    assert!(!other.finish_sample(picture, START, SampleObservation::Picture(&[0, 0])));
    assert!(!monitor.samples_pending());
    assert_eq!(monitor.summary(START).frames_sampled, 0);
    assert_eq!(other.summary(START).frames_sampled, 0);
}

#[test]
fn deadline_stop_during_initialization_cannot_rearm_and_cleanup_is_idempotent() {
    let mut monitor = core(MonitorOptions::default());
    assert_eq!(monitor.finish_cleanup(), Err(CleanupError::NotStopping));
    assert!(monitor.begin_stop(START + 500));
    assert!(!monitor.complete_setup(START + 1000, true));
    monitor.load_program_scene(&json!({"sceneName":"late"}));
    monitor.on_disconnect(START + 1000);
    assert!(monitor.begin_sample(SampleKind::Stats).is_none());
    monitor.observe_stats(START + 1000, &json!({"renderSkippedFrames":0}));
    monitor.observe_stats(START + 1100, &json!({"renderSkippedFrames":1}));
    monitor.finish_cleanup().unwrap();
    assert_eq!(monitor.phase(), Phase::Stopped);
    assert!(!monitor.begin_stop(START + 9000));
    monitor.finish_cleanup().unwrap();
    assert_eq!(monitor.summary(START + 9999).duration_seconds, 0.5);
    assert_eq!(monitor.summary(START + 9999).warnings.len(), 1);
    assert_eq!(monitor.summary(START + 9999).warnings[0].at_seconds, 0.5);
}

#[test]
fn cleanup_waits_for_inflight_samples_and_late_picture_cannot_publish() {
    let mut monitor = core(MonitorOptions::default());
    monitor.load_program_scene(&json!({"sceneName":"Program"}));
    monitor.complete_setup(START, true);
    let picture = monitor.begin_sample(SampleKind::Picture).unwrap();
    let stats = monitor.begin_sample(SampleKind::Stats).unwrap();
    monitor.begin_stop(START + 1000);
    assert_eq!(monitor.finish_cleanup(), Err(CleanupError::SamplesPending));
    // The owned ticket finishes, but the stopped guard discards these pixels.
    assert!(monitor.finish_sample(picture, START + 1500, SampleObservation::Picture(&[0, 0])));
    assert_eq!(monitor.summary(START + 1500).frames_sampled, 0);
    assert_eq!(monitor.finish_cleanup(), Err(CleanupError::SamplesPending));
    drop(stats);
    monitor.finish_cleanup().unwrap();
    assert_eq!(monitor.summary(START + 2000).duration_seconds, 1.0);
    let before = serde_json::to_value(monitor.summary(START)).unwrap();
    monitor.observe_stats(START + 3000, &json!({"renderSkippedFrames":999}));
    monitor.observe_picture(START + 3000, &[0, 0]);
    monitor.on_event(
        START + 3000,
        "RecordStateChanged",
        &json!({"outputPath":"late"}),
    );
    assert_eq!(
        serde_json::to_value(monitor.summary(START)).unwrap(),
        before
    );
}

#[test]
fn sample_failure_releases_admission_and_paused_picture_admission_stops() {
    let mut monitor = core(MonitorOptions::default());
    monitor.load_program_scene(&json!({"sceneName":"Program"}));
    monitor.complete_setup(START, true);
    let stats = monitor.begin_sample(SampleKind::Stats).unwrap();
    assert!(monitor.finish_sample(stats, START, SampleObservation::Failed));
    assert!(!monitor.samples_pending());
    monitor.on_event(
        START + 1000,
        "RecordStateChanged",
        &json!({"outputState":"OBS_WEBSOCKET_OUTPUT_PAUSED"}),
    );
    assert!(monitor.begin_sample(SampleKind::Picture).is_none());
    let stats = monitor.begin_sample(SampleKind::Stats).unwrap();
    assert!(!monitor.finish_sample(stats, START, SampleObservation::Picture(&[0, 0])));
    assert!(!monitor.samples_pending());
}
