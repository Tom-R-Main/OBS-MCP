// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::{
    media_backend::{qualified_encode_args, qualified_probe_args},
    media_verification::{LocalMediaInput, local_media_input_options},
};
use obs_mcp_native::{
    media_verification::{self, Cancellation, ProcessOptions},
    trim::*,
};
use serde_json::{Value, json};
use std::{
    ffi::OsString,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::{Instant, timeout};
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "trim-contract-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&path).unwrap();
        Self(path.canonicalize().unwrap())
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn node(input: &Value) -> Value {
    let script = r#"
import fs from'node:fs';import cp from'node:child_process';import{promisify}from'node:util';import{syncBuiltinESMExports}from'node:module';import{pathToFileURL}from'node:url';import{dirname,join}from'node:path';import{EventEmitter}from'node:events';
const chunks=[];for await(const c of process.stdin)chunks.push(c);const input=JSON.parse(Buffer.concat(chunks));const calls=[];let metadata;
const fake=async(command,args,options)=>{calls.push({command,args,timeout:options?.timeout,maxBuffer:options?.maxBuffer});if(command==='ffprobe')return{stdout:JSON.stringify(input.probeData??{}),stderr:''};if(args.includes('-encoders'))return{stdout:input.encoders??'libx264',stderr:''};if(args.includes('ffmetadata')){metadata=fs.readFileSync(args[args.indexOf('-i',args.indexOf('ffmetadata'))+1],'utf8');}if(input.action==='sheet')fs.writeFileSync(args.at(-1),'fixture-jpeg');return{stdout:'',stderr:''};};
cp.execFile=function(){};cp.execFile[promisify.custom]=fake;
if(input.action==='gray'||input.action==='tool-plan')cp.spawn=(command,args)=>{calls.push({command,args});const child=new EventEmitter();child.stdout=new EventEmitter();child.stderr=new EventEmitter();child.kill=()=>{};setImmediate(()=>{for(const chunk of input.chunks)child.stdout.emit('data',Buffer.from(chunk,'base64'));for(const chunk of input.stderrChunks??[])child.stderr.emit('data',Buffer.from(chunk));child.emit('close',0)});return child;};
syncBuiltinESMExports();const base=pathToFileURL(join(process.cwd(),'build/tools/trim.js'));let source=fs.readFileSync(base,'utf8').replace(/from\s*(['"])([^'"]+)\1/g,(_all,_q,spec)=>`from ${JSON.stringify(spec.startsWith('.')?new URL(spec,base).href:spec.startsWith('node:')?spec:import.meta.resolve(spec))}`);source+='\nexport{ffmetadata,exportTrimmed,parseSilence,probe,stillSpans,detectIdle,takeLogChapters};';const m=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
let result;try{switch(input.action){case'cuts':result=m.planCuts(input.options);break;case'remap':result=input.times.map(t=>m.remapTime(t,input.cuts));break;case'seams':result=m.seamSampleTimes(input.seams,input.duration);break;case'sdr':m.requireSdrInput(input.info);result={ok:true};break;case'validate':m.validateTrimmedMetadata(input.info,input.expected);result={ok:true};break;case'encoder':result=m.qualifiedEncoderArgs(input.encoders);break;case'metadata':result=m.ffmetadata(input.chapters,input.duration);break;case'silence':result=m.parseSilence(input.stderr,input.duration);break;case'probe':result=await m.probe(input.path);break;case'encode':await m.exportTrimmed(input.path,input.output,input.keep,input.audio,input.chapters,input.duration);result={calls,metadata};break;case'sheet':await m.contactSheet(input.path,input.times,input.columns,input.width);result={calls};break;case'gray':result=await m.stillSpans(input.path,input.info,input.minIdleSeconds,[],undefined);break;case'tool-plan':{const{McpServer,InMemoryTransport}=await import('@modelcontextprotocol/server');const{Client}=await import('@modelcontextprotocol/client');const server=new McpServer({name:'trim-oracle',version:'1'});m.initialize(server,{async sendRequest(type){if(type!=='GetRecordDirectory')throw Error('unexpected OBS method');return{recordDirectory:input.root}}});const client=new Client({name:'consumer',version:'1'});const[a,b]=InMemoryTransport.createLinkedPair();await Promise.all([server.connect(a),client.connect(b)]);try{result=await client.callTool({name:'obs-trim-take',arguments:{path:input.path,...input.options}})}finally{await client.close();await server.close();}break;}case'publish':{const root=input.root,partial=join(root,'partial.mp4'),output=join(root,'final.mp4'),abort=new AbortController(),events=[];try{const value=await m.publishTrimmedExport(partial,output,{encode:async()=>{events.push('encode');fs.writeFileSync(partial,'ours');if(input.mode==='trailer')throw Error('Error writing trailer');if(input.mode==='cancel-encode')abort.abort();},verify:async()=>{events.push('verify');if(input.mode==='decode')throw Error('strict decode failed');if(input.mode==='race')fs.writeFileSync(output,'theirs');if(input.mode==='cancel-verify')abort.abort();return input.info;},preview:input.mode==='preview'?async()=>{events.push('preview');throw Error('preview failed')}:undefined},abort.signal);result={ok:true,verified:value.verified,sha256:value.sha256,previewWarning:value.previewWarning,events};}catch(e){result={ok:false,error:e.message,code:e.code,events};}break;}default:throw Error('unknown action');}console.log(JSON.stringify({result,calls}));}catch(error){console.log(JSON.stringify({error:error.message,calls}));}
"#;
    let mut child = Command::new("node")
        .arg("--input-type=module")
        .arg("-e")
        .arg(script)
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let written = child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(input).unwrap());
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "trim oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    written.unwrap();
    serde_json::from_slice(&output.stdout).unwrap()
}
fn assert_json(actual: &Value, expected: &Value) {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => assert_eq!(a.as_f64(), b.as_f64()),
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len());
            for (a, b) in a.iter().zip(b) {
                assert_json(a, b)
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            assert_eq!(a.keys().collect::<Vec<_>>(), b.keys().collect::<Vec<_>>());
            for (k, v) in a {
                assert_json(v, &b[k])
            }
        }
        _ => assert_eq!(actual, expected),
    }
}
fn info() -> MediaProbe {
    serde_json::from_value(json!({"duration":9.5,"audioStreams":0,"videoStreams":1,"chapters":[{"at":0,"name":"Intro"},{"at":7,"name":"Answer"}],"width":160,"height":90,"codec":"h264","pixelFormat":"yuv420p"})).unwrap()
}
fn expected() -> ExpectedMetadata {
    let info = info();
    ExpectedMetadata {
        duration: info.duration,
        width: info.width,
        height: info.height,
        audio_streams: info.audio_streams,
        chapters: info.chapters,
    }
}
fn options() -> CutOptions {
    CutOptions {
        duration: 60.0,
        idle: vec![
            Span {
                start: 10.0,
                end: 30.0,
            },
            Span {
                start: 40.0,
                end: 60.0,
            },
        ],
        chapters: vec![Chapter {
            at: 30.0,
            name: "Next".into(),
        }],
        min_idle_seconds: 4.0,
        keep_seconds: 1.0,
        hold_after_chapter_seconds: 1.5,
        hold_before_chapter_seconds: 3.0,
    }
}
#[test]
fn source_timeline_cut_complement_remap_and_short_tail_seams_match() {
    let mut cases = vec![options()];
    let mut overlapping = options();
    overlapping.idle = vec![
        Span {
            start: -2.0,
            end: 20.0,
        },
        Span {
            start: 15.0,
            end: 50.0,
        },
    ];
    overlapping.chapters = vec![
        Chapter {
            at: 10.125,
            name: "One".into(),
        },
        Chapter {
            at: 33.335,
            name: "Two".into(),
        },
    ];
    cases.push(overlapping);
    let mut fragments = options();
    fragments.duration = 60.0;
    fragments.hold_before_chapter_seconds = 0.25;
    fragments.idle = vec![Span {
        start: 50.0,
        end: 60.0,
    }];
    fragments.chapters = vec![Chapter {
        at: 51.6,
        name: "x".into(),
    }];
    cases.push(fragments);
    for options in cases {
        let expected = node(&json!({"action":"cuts","options":options}));
        let plan = plan_cuts(&options);
        assert_json(&json!(plan), &expected["result"]);
        let times = [0.0, 5.0, 11.0, 15.0, 25.0, 40.0, 59.995, 60.0];
        assert_json(
            &json!(times.map(|t| remap_time(t, &plan.cuts))),
            &node(&json!({"action":"remap","times":times,"cuts":plan.cuts}))["result"],
        );
        let timeline = timeline("take.mp4", &options, true);
        assert_eq!(
            timeline.removed_seconds + timeline.trimmed_seconds,
            timeline.duration_seconds
        );
        for pair in plan.keep.windows(2) {
            assert!(pair[0].end <= pair[1].start);
        }
        assert!(
            plan_report(&timeline, 4.0, false)["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("Plan only")
        );
    }
    for (duration, seams) in [
        (2.11, vec![2.0]),
        (0.03, vec![0.01]),
        (100.0, (0..20).map(|i| i as f64 * 2.0).collect()),
    ] {
        assert_json(
            &json!(seam_sample_times(&seams, duration)),
            &node(&json!({"action":"seams","seams":seams,"duration":duration}))["result"],
        );
    }
}
#[test]
fn source_sdr_encoder_and_metadata_refusals_match_exactly() {
    let base = info();
    for change in [
        json!({}),
        json!({"colorTransfer":"smpte2084"}),
        json!({"colorTransfer":"arib-std-b67"}),
        json!({"pixelFormat":"yuv420p10le"}),
        json!({"bitDepth":10}),
        json!({"pixelFormat":null}),
    ] {
        let mut value = json!(base);
        value
            .as_object_mut()
            .unwrap()
            .extend(change.as_object().unwrap().clone());
        if value["pixelFormat"].is_null() {
            value.as_object_mut().unwrap().remove("pixelFormat");
        }
        let actual: MediaProbe = serde_json::from_value(value.clone()).unwrap();
        let oracle = node(&json!({"action":"sdr","info":value}));
        match require_sdr_input(&actual) {
            Ok(()) => assert_eq!(oracle["result"], json!({"ok":true})),
            Err(error) => assert_eq!(error.message(), oracle["error"].as_str().unwrap()),
        }
    }
    for encoders in [
        " V..... libx264 ",
        " V..... h264_videotoolbox ",
        "h264_nvenc mpeg4",
        "élibx264é",
        "not_libx264_suffix",
        "h264_videotoolbox libx264",
    ] {
        let oracle = node(&json!({"action":"encoder","encoders":encoders}));
        match qualified_encoder_args(encoders) {
            Ok(args) => assert_eq!(json!(args), oracle["result"]),
            Err(error) => assert_eq!(error.message(), oracle["error"].as_str().unwrap()),
        }
    }
    for change in [
        json!({}),
        json!({"duration":9.76}),
        json!({"videoStreams":0}),
        json!({"codec":"mpeg4"}),
        json!({"pixelFormat":"nv12"}),
        json!({"width":640}),
        json!({"audioStreams":1}),
        json!({"chapters":[]}),
        json!({"chapters":[{"at":0,"name":"Intro"},{"at":8,"name":"Answer"}]}),
    ] {
        let mut value = json!(base);
        value
            .as_object_mut()
            .unwrap()
            .extend(change.as_object().unwrap().clone());
        let actual = serde_json::from_value(value.clone()).unwrap();
        let oracle = node(&json!({"action":"validate","info":value,"expected":expected()}));
        match validate_trimmed_metadata(&actual, &expected()) {
            Ok(()) => assert_eq!(oracle["result"], json!({"ok":true})),
            Err(error) => assert_eq!(error.message(), oracle["error"].as_str().unwrap()),
        }
    }
}
#[test]
fn source_ffmetadata_and_common_audio_video_command_cuts_are_exact() {
    let chapters = vec![
        Chapter {
            at: 0.0005,
            name: "Intro\r[CHAPTER]\n=;#\\\0\u{7f}🙂".into(),
        },
        Chapter {
            at: 7.1255,
            name: "Answer".into(),
        },
    ];
    assert_eq!(
        ffmetadata(&chapters, 9.5),
        node(&json!({"action":"metadata","chapters":chapters,"duration":9.5}))["result"]
    );
    let root = Scratch::new();
    for audio in [0, 3] {
        let keep = vec![
            Span {
                start: 0.0,
                end: 3.85,
            },
            Span {
                start: 4.35,
                end: 8.0,
            },
        ];
        let source = node(
            &json!({"action":"encode","path":root.path("input.mov"),"output":root.path("partial.mp4"),"keep":keep,"audio":audio,"chapters":chapters,"duration":7.5,"encoders":"libx264"}),
        );
        let calls = source["result"]["calls"].as_array().unwrap();
        let encoded = calls
            .iter()
            .find(|call| {
                call["args"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|v| v == "-filter_complex")
            })
            .unwrap();
        let args = encoded["args"].as_array().unwrap();
        let metadata_at = args.iter().position(|v| v == "ffmetadata").unwrap();
        let at = args
            .iter()
            .enumerate()
            .skip(metadata_at)
            .find(|(_, v)| *v == "-i")
            .unwrap()
            .0
            + 1;
        let metadata_path = Path::new(args[at].as_str().unwrap());
        let plan = encode_command(
            &root.path("input.mov"),
            &root.path("partial.mp4"),
            metadata_path,
            EncodePlan {
                keep: &keep,
                audio_streams: audio,
                chapters: &chapters,
                duration: 7.5,
            },
            "libx264",
        )
        .unwrap();
        assert_eq!(
            json!(
                qualified_encode_args(&plan, metadata_path)
                    .unwrap()
                    .iter()
                    .map(|s| s.to_str().unwrap())
                    .collect::<Vec<_>>()
            ),
            encoded["args"]
        );
        assert_eq!(plan.metadata, source["result"]["metadata"]);
        assert_eq!(encoded["timeout"], 600000);
        assert_eq!(encoded["maxBuffer"], 32 * 1024 * 1024);
        let filter = args[args.iter().position(|v| v == "-filter_complex").unwrap() + 1]
            .as_str()
            .unwrap();
        assert!(!filter.contains("0:a:1"));
        assert!(!filter.contains("tone"));
    }
}
#[test]
fn source_probe_number_projection_and_contact_sheet_commands_match() {
    let raw = json!({"format":{"duration":"9.5"},"streams":[{"codec_type":"video","codec_name":"h264","width":"160","height":90,"pix_fmt":"yuv420p","bits_per_raw_sample":"8","color_transfer":"bt709","color_primaries":"bt709","color_space":"bt709","color_range":"tv"},{"codec_type":"audio"},{"codec_type":"audio"}],"chapters":[{"start_time":"0","tags":{"title":"Intro"}},{"start_time":"7"}]});
    let source = node(&json!({"action":"probe","path":"take.mp4","probeData":raw}));
    assert_json(&json!(parse_probe(&raw)), &source["result"]);
    assert_eq!(
        json!(
            qualified_probe_args(Path::new("take.mp4"))
                .iter()
                .map(|v| v.to_str().unwrap())
                .collect::<Vec<_>>()
        ),
        source["calls"][0]["args"]
    );
    assert_eq!(source["calls"][0]["timeout"], 15000);
    let root = Scratch::new();
    let times = vec![0.0, 7.2, 9.4];
    let source = node(
        &json!({"action":"sheet","path":root.path("take.mp4"),"times":times,"columns":2,"width":320}),
    );
    let calls = source["result"]["calls"].as_array().unwrap();
    let work = Path::new(
        calls[0]["args"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()
            .as_str()
            .unwrap(),
    )
    .parent()
    .unwrap();
    let request = SheetRequest {
        path: root.path("take.mp4"),
        times,
        columns: 2,
        width: 320,
        max_image_bytes: 1024,
    };
    assert_eq!(
        json!(
            contact_sheet_commands(&request, work)
                .unwrap()
                .into_iter()
                .enumerate()
                .map(|(index, mut args)| {
                    let at = args.iter().position(|arg| arg == "-i").unwrap();
                    args.splice(
                        at..at,
                        local_media_input_options(if index < request.times.len() {
                            LocalMediaInput::Recording
                        } else {
                            LocalMediaInput::Images
                        }),
                    );
                    args.into_iter()
                        .map(|value| value.into_string().unwrap())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        ),
        json!(calls.iter().map(|c| c["args"].clone()).collect::<Vec<_>>())
    );
    assert!(calls.iter().all(|c| c["timeout"] == 30000));
    assert_eq!(
        contact_sheet_times(None, &info().chapters, 9.5),
        vec![1.0, 8.0]
    );
    assert_eq!(
        contact_sheet_times(Some(&[10.0]), &[], 9.5),
        Vec::<f64>::new()
    );
}
#[test]
fn source_silence_parsing_and_audio_intersection_are_conservative() {
    let stderr = "silence_start: -0.02\nsilence_end: 3.5 | duration\nsilence_start: 6\n";
    assert_json(
        &json!(parse_silence(stderr, 10.0)),
        &node(&json!({"action":"silence","stderr":stderr,"duration":10}))["result"],
    );
    let still = vec![Span {
        start: 0.0,
        end: 8.0,
    }];
    let mut probe = info();
    probe.duration = 10.0;
    probe.audio_streams = 2;
    let idle = detect_idle(&still, stderr, &probe);
    assert_eq!(
        idle.idle,
        vec![
            Span {
                start: 0.0,
                end: 3.5
            },
            Span {
                start: 6.0,
                end: 8.0
            }
        ]
    );
    assert!(idle.audible);
    assert_eq!(
        detect_idle(&still, "silence_start: 0\nsilence_end: 10", &probe).idle,
        still
    );
    assert!(!detect_idle(&still, "silence_start: 0\nsilence_end: 10", &probe).audible);
    assert!(detect_idle(&still, "", &probe).idle.is_empty());
}
#[test]
fn source_gray_stream_uses_shared_medians_arbitrary_chunks_and_bounded_history() {
    use base64::Engine;
    let mut probe = info();
    probe.duration = 20.0;
    let request = sampling_command(Path::new("take.mp4"), &probe, 1.0, 1024 * 1024).unwrap();
    let mut raw = vec![];
    for frame in 0..30 {
        let level = if frame >= 16 {
            180
        } else if frame % 3 == 1 {
            110
        } else {
            100
        };
        raw.extend(vec![level; request.frame_bytes]);
    }
    raw.extend([7, 8, 9]);
    let chunks = raw
        .chunks(997)
        .map(|chunk| base64::engine::general_purpose::STANDARD.encode(chunk))
        .collect::<Vec<_>>();
    let oracle = node(
        &json!({"action":"gray","path":"take.mp4","info":probe,"minIdleSeconds":1,"chunks":chunks,"stderrChunks":["silence_start: 0\n","silence_end: 20\n"]}),
    );
    let mut sink = GraySink::new(&request, 100).unwrap();
    for chunk in raw.chunks(997) {
        sink.consume(chunk).unwrap();
    }
    sink.consume_stderr(b"silence_start: 0\n");
    sink.consume_stderr(b"silence_end: 20\n");
    let analysis = sink.finish().unwrap();
    let mut qualified = request.args.clone();
    let at = qualified.iter().position(|arg| arg == "-i").unwrap();
    qualified.splice(
        at..at,
        local_media_input_options(LocalMediaInput::Recording),
    );
    assert_eq!(
        json!(
            qualified
                .iter()
                .map(|arg| arg.to_str().unwrap())
                .collect::<Vec<_>>()
        ),
        oracle["calls"][0]["args"]
    );
    assert_json(&json!(analysis.spans), &oracle["result"]["spans"]);
    assert_eq!(analysis.stderr, oracle["result"]["stderr"]);
    assert_eq!(analysis.frames, 30);
    assert_eq!(analysis.trailing_bytes, 3);
    let mut limited = GraySink::new(&request, 1).unwrap();
    let mut failed = false;
    for frame in 0..120 {
        let level = if (frame / 10) % 2 == 0 { 20 } else { 200 };
        if limited.consume(&vec![level; request.frame_bytes]).is_err() {
            failed = true;
            break;
        }
    }
    assert!(failed, "stream must stop on span-history cap before EOF");
    assert!(sampling_command(Path::new("take"), &probe, 1.0, 1).is_err());
}
#[test]
fn actual_typescript_public_plan_report_matches_native_timeline() {
    use base64::Engine;
    let root = Scratch::new();
    let path = root.path("take.mp4");
    fs::write(&path, b"synthetic; process adapters are injected").unwrap();
    let raw = json!({"format":{"duration":"14"},"streams":[{"codec_type":"video","codec_name":"h264","width":160,"height":90,"pix_fmt":"yuv420p"}],"chapters":[{"start_time":"0","tags":{"title":"Intro"}},{"start_time":"11.5","tags":{"title":"Answer"}}]});
    let info = parse_probe(&raw);
    let request = sampling_command(&path, &info, 4.0, 1024 * 1024).unwrap();
    let mut bytes = vec![];
    for frame in 0..28 {
        bytes.extend(vec![
            if frame < 6 {
                20
            } else if frame < 22 {
                100
            } else {
                180
            };
            request.frame_bytes
        ]);
    }
    let chunks = bytes
        .chunks(517)
        .map(|c| base64::engine::general_purpose::STANDARD.encode(c))
        .collect::<Vec<_>>();
    let expected = node(
        &json!({"action":"tool-plan","root":root.0,"path":path,"probeData":raw,"chunks":chunks,"options":{}}),
    );
    let mut sink = GraySink::new(&request, 100).unwrap();
    for chunk in bytes.chunks(517) {
        sink.consume(chunk).unwrap();
    }
    let sampled = sink.finish().unwrap();
    let options = CutOptions {
        duration: info.duration,
        idle: sampled.spans,
        chapters: info.chapters,
        min_idle_seconds: 4.0,
        keep_seconds: 1.0,
        hold_before_chapter_seconds: 3.0,
        hold_after_chapter_seconds: 1.5,
    };
    assert_json(
        &plan_report(
            &timeline(path.to_str().unwrap(), &options, false),
            4.0,
            false,
        ),
        &expected["result"],
    );
}

struct Ready<T: Clone + Send> {
    value: Option<Result<T, TrimError>>,
    action: Option<Box<dyn FnOnce() -> Result<T, TrimError> + Send>>,
}
impl<T: Clone + Send> Ready<T> {
    fn action(action: impl FnOnce() -> Result<T, TrimError> + Send + 'static) -> Self {
        Self {
            value: None,
            action: Some(Box::new(action)),
        }
    }
}
impl<T: Clone + Send> OwnedJob<T> for Ready<T> {
    fn cancel(&self) {}
    fn finish(&mut self) -> JobFuture<'_, T> {
        Box::pin(async move {
            if self.value.is_none() {
                self.value = Some(self.action.take().unwrap()());
            }
            self.value.clone().unwrap()
        })
    }
}
struct ProcessJob {
    process: media_verification::MediaProcess,
}
impl OwnedJob<()> for ProcessJob {
    fn cancel(&self) {
        self.process.cancel()
    }
    fn finish(&mut self) -> JobFuture<'_, ()> {
        Box::pin(async {
            self.process
                .finish()
                .await
                .map_err(|e| TrimError::new(ErrorKind::Encode, e.to_string()))
        })
    }
}
struct Backend {
    mode: String,
    output: PathBuf,
    events: Arc<Mutex<Vec<String>>>,
    probe: MediaProbe,
}
impl Encode for Backend {
    fn encode(
        &mut self,
        request: &EncodeRequest,
        cancel: Cancellation,
    ) -> Result<Box<dyn OwnedJob<()>>, TrimError> {
        self.events.lock().unwrap().push("encode".into());
        let path = request.output.clone();
        let mode = self.mode.clone();
        if mode == "hold" {
            let pid = path.with_extension("pid");
            let process=media_verification::run_bounded_media_process("node",&[OsString::from("-e"),OsString::from("process.on('SIGTERM',()=>{});require('fs').writeFileSync(process.argv[1],'ours');require('fs').writeFileSync(process.argv[2],String(process.pid));setInterval(()=>{},1000)"),path.into_os_string(),pid.into_os_string()],ProcessOptions{timeout:Duration::from_secs(5),cancellation:cancel}).map_err(|e|TrimError::new(ErrorKind::Encode,e.to_string()))?;
            return Ok(Box::new(ProcessJob { process }));
        }
        if mode == "trailer" {
            let process=media_verification::run_bounded_media_process("node",&[OsString::from("-e"),OsString::from("require('fs').writeFileSync(process.argv[1],'ours');console.error('progress=end');console.error('Error writing trailer');process.exitCode=1"),path.into_os_string()],ProcessOptions{timeout:Duration::from_secs(3),cancellation:cancel}).map_err(|e|TrimError::new(ErrorKind::Encode,e.to_string()))?;
            return Ok(Box::new(ProcessJob { process }));
        }
        Ok(Box::new(Ready::action(move || {
            fs::write(&path, b"ours").unwrap();
            if mode == "cancel-encode" {
                cancel.cancel();
            }
            Ok(())
        })))
    }
}
impl Probe for Backend {
    fn probe(
        &mut self,
        _: &Path,
        _: Cancellation,
    ) -> Result<Box<dyn OwnedJob<MediaProbe>>, TrimError> {
        self.events.lock().unwrap().push("verify".into());
        let mode = self.mode.clone();
        let output = self.output.clone();
        let info = self.probe.clone();
        Ok(Box::new(Ready::action(move || {
            if mode == "race" {
                fs::write(&output, b"theirs").unwrap();
            }
            Ok(info)
        })))
    }
}
impl Decode for Backend {
    fn decode(
        &mut self,
        path: &Path,
        cancel: Cancellation,
    ) -> Result<Box<dyn OwnedJob<()>>, TrimError> {
        self.events.lock().unwrap().push("decode".into());
        let mode = self.mode.clone();
        let path = path.to_owned();
        Ok(Box::new(Ready::action(move || {
            if mode == "decode" {
                return Err(TrimError::new(ErrorKind::Decode, "strict decode failed"));
            }
            if mode == "cancel-verify" {
                cancel.cancel();
            }
            if mode == "replace-stage" {
                fs::rename(&path, path.with_extension("old-owned")).unwrap();
                fs::write(&path, b"replacement").unwrap();
            }
            Ok(())
        })))
    }
}
impl Preview for Backend {
    fn preview(
        &mut self,
        _: &SheetRequest,
        _: Cancellation,
    ) -> Result<Box<dyn OwnedJob<Arc<[u8]>>>, TrimError> {
        self.events.lock().unwrap().push("preview".into());
        Ok(Box::new(Ready::action(|| {
            Err(TrimError::new(ErrorKind::Preview, "preview failed"))
        })))
    }
}
fn owner(
    root: &Scratch,
    mode: &str,
    cleanup: Option<Box<dyn StageCleanup>>,
) -> (ExportOwner<Backend>, Arc<Mutex<Vec<String>>>) {
    let stage = if let Some(cleanup) = cleanup {
        ExportStage::with_cleanup(&root.0, "partial.mp4", "final.mp4", cleanup).unwrap()
    } else {
        ExportStage::new(&root.0, "partial.mp4", "final.mp4").unwrap()
    };
    let request = encode_command(
        &root.path("source.mp4"),
        stage.partial_path(),
        &root.path("chapters.txt"),
        EncodePlan {
            keep: &[Span {
                start: 0.0,
                end: 9.5,
            }],
            audio_streams: 0,
            chapters: &info().chapters,
            duration: 9.5,
        },
        "libx264",
    )
    .unwrap();
    let events = Arc::new(Mutex::new(vec![]));
    let backend = Backend {
        mode: mode.into(),
        output: stage.output_path().to_owned(),
        events: events.clone(),
        probe: info(),
    };
    let preview = if mode == "preview" {
        Some(SheetRequest {
            path: stage.output_path().to_owned(),
            times: vec![1.0],
            columns: 1,
            width: 80,
            max_image_bytes: 1024,
        })
    } else {
        None
    };
    (
        ExportOwner::new(
            backend,
            stage,
            request,
            &info(),
            expected(),
            preview,
            Cancellation::default(),
        )
        .unwrap(),
        events,
    )
}
#[tokio::test]
async fn actual_source_publication_hash_preview_warning_and_verified_identity_match() {
    let native = Scratch::new();
    let source = Scratch::new();
    let expected =
        node(&json!({"action":"publish","root":source.0,"mode":"preview","info":info()}))["result"]
            .clone();
    let (mut owner, events) = owner(&native, "preview", None);
    let receipt = owner.finish().await.unwrap();
    assert_eq!(receipt.file.sha256, expected["sha256"]);
    assert_json(&json!(receipt.verified), &expected["verified"]);
    assert_eq!(
        receipt.preview_warning,
        expected["previewWarning"].as_str().map(str::to_owned)
    );
    assert!(receipt.file.still_names_committed_file());
    assert_eq!(fs::read(native.path("final.mp4")).unwrap(), b"ours");
    assert!(!native.path("partial.mp4").exists());
    assert_eq!(
        events.lock().unwrap().as_slice(),
        ["encode", "verify", "decode", "preview"]
    );
    let second = owner.finish().await.unwrap();
    assert_eq!(second.file.sha256, receipt.file.sha256);
    assert_eq!(events.lock().unwrap().len(), 4);
}
#[tokio::test]
async fn source_decode_terminal_trailer_cancel_and_destination_race_withhold_publication() {
    for mode in [
        "decode",
        "trailer",
        "cancel-encode",
        "cancel-verify",
        "race",
    ] {
        let native = Scratch::new();
        let source = Scratch::new();
        let expected = node(&json!({"action":"publish","root":source.0,"mode":mode,"info":info()}));
        assert_eq!(expected["result"]["ok"], false);
        let (mut owner, events) = owner(&native, mode, None);
        let error = owner.finish().await.err().unwrap();
        assert!(!native.path("partial.mp4").exists());
        if mode == "race" {
            assert_eq!(error.kind, ErrorKind::DestinationExists);
            assert_eq!(fs::read(native.path("final.mp4")).unwrap(), b"theirs");
        } else {
            assert!(!native.path("final.mp4").exists());
        }
        if mode == "trailer" {
            assert!(error.message().contains("Error writing trailer"));
            assert_eq!(events.lock().unwrap().as_slice(), ["encode"]);
        }
        if mode == "decode" {
            assert_eq!(
                error.message(),
                expected["result"]["error"].as_str().unwrap()
            );
        }
    }
}
#[tokio::test]
async fn destination_symlinks_and_replaced_stage_are_never_followed_or_cleaned_as_owned() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let root = Scratch::new();
        let other = Scratch::new();
        symlink(other.path("nonexistent"), root.path("final.mp4")).unwrap();
        let (mut export, _) = owner(&root, "normal", None);
        assert_eq!(
            export.finish().await.err().unwrap().kind,
            ErrorKind::DestinationExists
        );
        assert!(
            fs::symlink_metadata(root.path("final.mp4"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(!other.path("nonexistent").exists());
        assert!(!root.path("partial.mp4").exists());
    }
    let root = Scratch::new();
    let (mut owner, _) = owner(&root, "replace-stage", None);
    let error = owner.finish().await.err().unwrap();
    assert_eq!(error.kind, ErrorKind::StageChanged);
    assert!(error.cleanup_issue.is_some());
    assert!(!root.path("final.mp4").exists());
    assert_eq!(fs::read(root.path("partial.mp4")).unwrap(), b"replacement");
    assert_eq!(fs::read(root.path("partial.old-owned")).unwrap(), b"ours");
}
struct DeniedCleanup;
impl StageCleanup for DeniedCleanup {
    fn remove(&mut self, _: &Path) -> std::io::Result<()> {
        Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
    }
}
#[tokio::test]
async fn cleanup_failure_cannot_erase_a_committed_export() {
    let root = Scratch::new();
    let (mut owner, _) = owner(&root, "normal", Some(Box::new(DeniedCleanup)));
    let receipt = owner.finish().await.unwrap();
    assert!(
        receipt
            .cleanup_warning
            .as_ref()
            .unwrap()
            .starts_with("Export committed;")
    );
    assert!(receipt.file.still_names_committed_file());
    assert!(root.path("partial.mp4").exists());
    assert_eq!(fs::read(root.path("final.mp4")).unwrap(), b"ours");
}
#[tokio::test]
async fn cancelled_finish_retains_one_encoder_then_joins_before_cleanup() {
    let root = Scratch::new();
    let (mut owner, events) = owner(&root, "hold", None);
    assert!(
        timeout(Duration::from_millis(150), owner.finish())
            .await
            .is_err()
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while !root.path("partial.pid").exists() {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let pid = fs::read_to_string(root.path("partial.pid")).unwrap();
    assert!(root.path("partial.mp4").exists());
    assert!(!root.path("final.mp4").exists());
    owner.cancel();
    let error = timeout(Duration::from_secs(3), owner.finish())
        .await
        .unwrap()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert!(!root.path("partial.mp4").exists());
    assert!(!root.path("final.mp4").exists());
    assert_eq!(events.lock().unwrap().as_slice(), ["encode"]);
    #[cfg(unix)]
    assert!(
        !Command::new("/bin/kill")
            .args(["-0", "--", pid.trim()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success(),
        "publication cleanup preceded direct-child reap"
    );
}
#[tokio::test]
async fn actual_bounded_stdout_actor_feeds_gray_sink_and_caches_arc_after_both_eofs() {
    let request = sampling_command(Path::new("take.mp4"), &info(), 1.0, 1024 * 1024).unwrap();
    let script = "process.stderr.write('silence_start: 0\\nsilence_end: 10\\n');for(let i=0;i<20;i++)process.stdout.write(Buffer.alloc(Number(process.argv[1]),100));";
    let sink = GraySink::new(&request, 100).unwrap();
    let mut process = media_verification::run_streamed_media_process(
        "node",
        &[
            OsString::from("-e"),
            OsString::from(script),
            OsString::from(request.frame_bytes.to_string()),
        ],
        ProcessOptions {
            timeout: Duration::from_secs(3),
            ..ProcessOptions::default()
        },
        sink,
    )
    .unwrap();
    let first = process.finish().await.unwrap();
    assert_eq!(first.frames, 20);
    assert!(first.stderr.contains("silence_end: 10"));
    assert_eq!(first.trailing_bytes, 0);
    assert_eq!(
        first.spans,
        vec![Span {
            start: 0.0,
            end: 9.5
        }]
    );
    let second = process.finish().await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
}
