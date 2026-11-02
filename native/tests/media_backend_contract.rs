// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::{
    media_backend::{Bounds, SystemMedia},
    media_verification::{self, Cancellation, ProcessOptions},
    trim::{
        self, Decode, Encode, EncodePlan, ErrorKind, ExpectedMetadata, ExportOwner, ExportStage,
        MediaProbe, Preview, Probe, SheetRequest, Span,
    },
};
use std::{
    ffi::OsString,
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::{Instant, timeout};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "media-backend-contract-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&path).unwrap();
        private(&path);
        Self(path.canonicalize().unwrap())
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
    fn work(&self) -> PathBuf {
        let path = self.path("work");
        fs::create_dir(&path).unwrap();
        private(&path);
        path
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn private(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
}
fn tool(name: &str) -> PathBuf {
    ["/opt/homebrew/bin", "/usr/local/bin", "/usr/bin"]
        .into_iter()
        .map(|root| Path::new(root).join(name))
        .find(|path| path.is_file())
        .unwrap_or_else(|| {
            panic!("installed {name} is required for the concrete media qualification")
        })
}
fn backend(work: &Path) -> SystemMedia {
    SystemMedia::new(&tool("ffmpeg"), &tool("ffprobe"), work, Bounds::default()).unwrap()
}
fn args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}
async fn run_ffmpeg(arguments: &[OsString]) {
    let mut job = media_verification::run_bounded_media_process(
        tool("ffmpeg"),
        arguments,
        ProcessOptions {
            timeout: Duration::from_secs(20),
            ..ProcessOptions::default()
        },
    )
    .unwrap();
    job.finish().await.unwrap();
}
async fn generate(root: &Scratch) -> PathBuf {
    let path = root.path("source.mkv");
    let mut arguments = args(&[
        "-v",
        "error",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "color=c=red:s=160x90:r=30:d=2",
        "-f",
        "lavfi",
        "-i",
        "color=c=blue:s=160x90:r=30:d=2",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=1000:sample_rate=48000:duration=4",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=2000:sample_rate=48000:duration=4",
        "-filter_complex",
        "[0:v:0][1:v:0]concat=n=2:v=1:a=0[v]",
        "-map",
        "[v]",
        "-map",
        "2:a:0",
        "-map",
        "3:a:0",
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-t",
        "4",
    ]);
    arguments.push(path.as_os_str().to_owned());
    run_ffmpeg(&arguments).await;
    path
}
async fn encoder_list(media: &SystemMedia) -> String {
    let mut job = media.encoders(Cancellation::default()).unwrap();
    let first = job.finish().await.unwrap();
    let second = job.finish().await.unwrap();
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    String::from_utf8(first.to_vec()).unwrap()
}
async fn probe(media: &mut SystemMedia, path: &Path) -> MediaProbe {
    let mut job = media.probe(path, Cancellation::default()).unwrap();
    let first = job.finish().await.unwrap();
    let second = job.finish().await.unwrap();
    assert_eq!(first.duration, second.duration);
    first
}
async fn capture_ffmpeg(arguments: &[OsString], limit: usize) -> std::sync::Arc<[u8]> {
    let mut process = media_verification::capture_bounded_media_process(
        tool("ffmpeg"),
        arguments,
        ProcessOptions {
            timeout: Duration::from_secs(10),
            ..ProcessOptions::default()
        },
        limit,
    )
    .unwrap();
    process.finish().await.unwrap()
}
#[tokio::test]
async fn installed_ffmpeg_encodes_sdr_common_cuts_chapters_first_audio_and_publishes_after_strict_decode()
 {
    let root = Scratch::new();
    let work = root.work();
    let source = generate(&root).await;
    let mut media = backend(&work);
    let input = probe(&mut media, &source).await;
    assert_eq!(input.audio_streams, 2);
    assert_eq!(input.video_streams, 1);
    trim::require_sdr_input(&input).unwrap();
    let encoders = encoder_list(&media).await;
    assert!(encoders.contains("libx264"));
    let chapters = vec![
        trim::Chapter {
            at: 0.0,
            name: "Red".into(),
        },
        trim::Chapter {
            at: 1.5,
            name: "Blue".into(),
        },
    ];
    let stage = ExportStage::new(&root.0, "partial.mp4", "final.mp4").unwrap();
    let request = trim::encode_command(
        &source,
        stage.partial_path(),
        &root.path("must-not-be-created.txt"),
        EncodePlan {
            keep: &[
                Span {
                    start: 0.0,
                    end: 1.0,
                },
                Span {
                    start: 1.5,
                    end: 4.0,
                },
            ],
            audio_streams: input.audio_streams,
            chapters: &chapters,
            duration: 3.5,
        },
        &encoders,
    )
    .unwrap();
    let expected = ExpectedMetadata {
        duration: 3.5,
        width: input.width,
        height: input.height,
        audio_streams: input.audio_streams,
        chapters,
    };
    let cancellation = Cancellation::default();
    let mut owner = ExportOwner::new(
        media,
        stage,
        request,
        &input,
        expected,
        Some(SheetRequest {
            path: root.path("final.mp4"),
            times: vec![1.35, 1.65],
            columns: 4,
            width: 320,
            max_image_bytes: 1024 * 1024,
        }),
        cancellation.clone(),
    )
    .unwrap();
    let exported = owner.finish().await.unwrap();
    assert_eq!(exported.verified.audio_streams, 1);
    assert_eq!(exported.verified.chapters.len(), 2);
    assert_eq!(exported.verified.codec.as_deref(), Some("h264"));
    assert_eq!(exported.verified.pixel_format.as_deref(), Some("yuv420p"));
    assert!(exported.file.still_names_committed_file());
    assert!(exported.cleanup_warning.is_none());
    assert!(
        exported
            .preview
            .as_ref()
            .is_some_and(|bytes| !bytes.is_empty())
    );
    assert!(exported.preview_warning.is_none());
    assert_eq!(exported.file.sha256.len(), 64);
    drop(owner);
    assert!(
        !cancellation.is_cancelled(),
        "completed export owner cancelled its successor"
    );
    assert!(fs::read_dir(&work).unwrap().next().is_none());
    assert!(!root.path("must-not-be-created.txt").exists());
    assert!(!root.path("partial.mp4").exists());
    let output = root.path("final.mp4");
    let mut media = backend(&work);
    media
        .decode(&output, Cancellation::default())
        .unwrap()
        .finish()
        .await
        .unwrap();
    let original_bytes = fs::read(&output).unwrap();
    for (time, red) in [("0.5", true), ("2.5", false)] {
        let mut arguments = args(&["-v", "error", "-ss", time, "-i"]);
        arguments.push(output.as_os_str().to_owned());
        arguments.extend(args(&[
            "-frames:v",
            "1",
            "-vf",
            "scale=1:1,format=rgb24",
            "-f",
            "rawvideo",
            "pipe:1",
        ]));
        let pixel = capture_ffmpeg(&arguments, 16).await;
        assert_eq!(pixel.len(), 3);
        if red {
            assert!(pixel[0] > 100 && pixel[2] < 30)
        } else {
            assert!(pixel[2] > 100 && pixel[0] < 30)
        }
    }
    // The first source track is1000Hz, the second2000Hz. Check a bounded steady
    // 0.2s PCM window so stream-count success cannot conceal the wrong track.
    let mut arguments = args(&["-v", "error", "-i"]);
    arguments.push(output.as_os_str().to_owned());
    arguments.extend(args(&[
        "-map",
        "0:a:0",
        "-af",
        "atrim=start=0.3:end=0.5",
        "-ac",
        "1",
        "-ar",
        "48000",
        "-f",
        "s16le",
        "pipe:1",
    ]));
    let pcm = capture_ffmpeg(&arguments, 24000).await;
    let values = pcm
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]))
        .collect::<Vec<_>>();
    let rising = values
        .windows(2)
        .filter(|pair| pair[0] <= 0 && pair[1] > 0)
        .count();
    assert!(
        (185..=215).contains(&rising),
        "first-audio frequency was not preserved"
    );
    // A second encode can finish, but atomic publication still refuses overwrite.
    let stage = ExportStage::new(&root.0, "second.partial.mp4", "final.mp4").unwrap();
    let request = trim::encode_command(
        &source,
        stage.partial_path(),
        &root.path("not-used.txt"),
        EncodePlan {
            keep: &[Span {
                start: 0.0,
                end: 4.0,
            }],
            audio_streams: 2,
            chapters: &[],
            duration: 4.0,
        },
        &encoders,
    )
    .unwrap();
    let mut second = ExportOwner::new(
        media,
        stage,
        request,
        &input,
        ExpectedMetadata {
            duration: 4.0,
            width: input.width,
            height: input.height,
            audio_streams: 2,
            chapters: vec![],
        },
        None,
        Cancellation::default(),
    )
    .unwrap();
    assert_eq!(
        second.finish().await.err().unwrap().kind,
        ErrorKind::DestinationExists
    );
    assert_eq!(fs::read(&output).unwrap(), original_bytes);
    assert!(fs::read_dir(&work).unwrap().next().is_none());
}
#[tokio::test]
async fn actual_ffmpeg_terminal_failure_and_pre_cancel_never_publish_or_leave_metadata() {
    let root = Scratch::new();
    let work = root.work();
    let source = root.path("broken.mkv");
    fs::write(&source, b"not a media container").unwrap();
    let media = backend(&work);
    let encoders = encoder_list(&media).await;
    let mut info = trim::parse_probe(
        &serde_json::json!({"format":{"duration":"1"},"streams":[{"codec_type":"video","width":160,"height":90,"pix_fmt":"yuv420p","codec_name":"h264"}]}),
    );
    info.audio_streams = 0;
    let stage = ExportStage::new(&root.0, "partial.mp4", "final.mp4").unwrap();
    let request = trim::encode_command(
        &source,
        stage.partial_path(),
        &root.path("not-used"),
        EncodePlan {
            keep: &[Span {
                start: 0.0,
                end: 1.0,
            }],
            audio_streams: 0,
            chapters: &[],
            duration: 1.0,
        },
        &encoders,
    )
    .unwrap();
    let mut owner = ExportOwner::new(
        media,
        stage,
        request,
        &info,
        ExpectedMetadata {
            duration: 1.0,
            width: info.width,
            height: info.height,
            audio_streams: 0,
            chapters: vec![],
        },
        None,
        Cancellation::default(),
    )
    .unwrap();
    let error = owner.finish().await.err().unwrap();
    assert_eq!(error.kind, ErrorKind::Encode);
    assert!(error.cleanup_issue.is_none());
    assert!(!root.path("final.mp4").exists());
    assert!(!root.path("partial.mp4").exists());
    assert!(fs::read_dir(&work).unwrap().next().is_none());
    let mut media = backend(&work);
    let stage = ExportStage::new(&root.0, "cancel.partial.mp4", "cancel.final.mp4").unwrap();
    let request = trim::encode_command(
        &source,
        stage.partial_path(),
        &root.path("not-used"),
        EncodePlan {
            keep: &[Span {
                start: 0.0,
                end: 1.0,
            }],
            audio_streams: 0,
            chapters: &[],
            duration: 1.0,
        },
        &encoders,
    )
    .unwrap();
    let cancellation = Cancellation::default();
    cancellation.cancel();
    let failure = media.encode(&request, cancellation).err().unwrap();
    assert_eq!(failure.kind, ErrorKind::Cancelled);
    assert!(fs::read_dir(&work).unwrap().next().is_none());
    media
        .decode(&source, Cancellation::default())
        .unwrap()
        .finish()
        .await
        .unwrap_err();
    assert!(!root.path("cancel.final.mp4").exists());
}
#[tokio::test]
async fn bounded_probe_refuses_output_overflow_and_malformed_media_without_writes() {
    let root = Scratch::new();
    let work = root.work();
    let source = generate(&root).await;
    let bounds = Bounds {
        probe_stdout_bytes: 16,
        ..Bounds::default()
    };
    let mut media = SystemMedia::new(&tool("ffmpeg"), &tool("ffprobe"), &work, bounds).unwrap();
    let mut job = media.probe(&source, Cancellation::default()).unwrap();
    let first = job.finish().await.unwrap_err();
    assert_eq!(first.kind, ErrorKind::Probe);
    assert!(first.message().contains("byte limit"));
    let second = job.finish().await.unwrap_err();
    assert_eq!(first, second);
    assert!(fs::read_dir(&work).unwrap().next().is_none());
}
#[tokio::test]
async fn actual_ffmpeg_cancel_after_spawn_joins_before_metadata_cleanup() {
    let root = Scratch::new();
    let work = root.work();
    let source = generate(&root).await;
    let mut media = backend(&work);
    let stage = ExportStage::new(&root.0, "partial.mp4", "final.mp4").unwrap();
    let request = trim::encode_command(
        &source,
        stage.partial_path(),
        &root.path("unused"),
        EncodePlan {
            keep: &[Span {
                start: 0.0,
                end: 4.0,
            }],
            audio_streams: 2,
            chapters: &[],
            duration: 4.0,
        },
        "libx264",
    )
    .unwrap();
    let cancellation = Cancellation::default();
    let mut job = media.encode(&request, cancellation.clone()).unwrap();
    assert_eq!(fs::read_dir(&work).unwrap().count(), 1);
    cancellation.cancel();
    let error = job.finish().await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert!(error.cleanup_issue.is_none());
    assert!(fs::read_dir(&work).unwrap().next().is_none());
    assert!(!root.path("final.mp4").exists());
    assert_eq!(job.finish().await.unwrap_err(), error);
}
#[cfg(unix)]
fn helper(root: &Scratch) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = root.path("owned-ffmpeg-fixture.cjs");
    fs::write(&path,r#"#!/usr/bin/env node
const fs=require('node:fs'),p=require('node:path');const args=process.argv.slice(2),at=args.indexOf('ffmetadata'),metadata=args[args.indexOf('-i',at)+1],root=p.dirname(process.argv[1]);
process.on('SIGTERM',()=>{});fs.appendFileSync(p.join(root,'spawns'),'spawn\n');
const publish=()=>{fs.writeFileSync(p.join(root,'metadata-path.tmp'),metadata);fs.renameSync(p.join(root,'metadata-path.tmp'),p.join(root,'metadata-path'));fs.writeFileSync(p.join(root,'ready.tmp'),String(process.pid));fs.renameSync(p.join(root,'ready.tmp'),p.join(root,'ready'));};
if(fs.existsSync(p.join(root,'hold-metadata'))){fs.writeFileSync(p.join(root,'metadata-paused'),'paused');const gate=setInterval(()=>{if(!fs.existsSync(p.join(root,'hold-metadata'))){clearInterval(gate);publish();}},5);}else publish();
setInterval(()=>{},1000);
"#).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path
}
async fn wait_file(path: &Path) -> bool {
    let until = Instant::now() + Duration::from_secs(3);
    while !path.exists() {
        if Instant::now() >= until {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    true
}
// A readiness timeout or early result is evidence, not permission to unwind
// while the caller still owns live work. Return after dropping the frontend
// borrow so each failure branch can cancel and join the exact same job first.
async fn wait_for_job_readiness<T: std::fmt::Debug>(
    job: &mut dyn trim::OwnedJob<T>,
    path: &Path,
) -> Result<(), String> {
    let mut frontend = job.finish();
    tokio::select! {
        ready=wait_file(path)=>if ready {Ok(())} else {Err("owned child did not reach ready state within its 3s startup budget".into())},
        result=&mut frontend=>Err(format!("owned job completed before readiness: {result:?}")),
    }
}
#[cfg(unix)]
#[tokio::test]
async fn abandoned_await_keeps_one_child_and_metadata_until_cancelled_child_is_reaped() {
    let root = Scratch::new();
    let work = root.work();
    let helper = helper(&root);
    fs::write(root.path("source.mkv"), b"owned process-lifetime fixture").unwrap();
    let mut media = SystemMedia::new(&helper, &tool("ffprobe"), &work, Bounds::default()).unwrap();
    let stage = ExportStage::new(&root.0, "partial.mp4", "final.mp4").unwrap();
    let request = trim::encode_command(
        &root.path("source.mkv"),
        stage.partial_path(),
        &root.path("unused"),
        EncodePlan {
            keep: &[Span {
                start: 0.0,
                end: 1.0,
            }],
            audio_streams: 0,
            chapters: &[],
            duration: 1.0,
        },
        "libx264",
    )
    .unwrap();
    let mut job = media.encode(&request, Cancellation::default()).unwrap();
    assert_eq!(fs::read_dir(&work).unwrap().count(), 1);
    let abandoned_wait = timeout(Duration::from_millis(100), job.finish()).await;
    if abandoned_wait.is_ok() {
        job.cancel();
        let joined = job.finish().await;
        panic!(
            "owned encode completed before the abandoned wait: {abandoned_wait:?}; joined: {joined:?}"
        );
    }
    let readiness = wait_for_job_readiness(job.as_mut(), &root.path("ready")).await;
    if readiness.is_err() {
        job.cancel();
        let joined = job.finish().await;
        panic!("encode readiness failed after joined cleanup: {readiness:?}; joined: {joined:?}");
    }
    // Capture these historical facts while the child is alive, but join the
    // owned child before unwrapping fixture evidence or asserting its shape.
    let pid = fs::read_to_string(root.path("ready"));
    let metadata = fs::read_to_string(root.path("metadata-path")).map(PathBuf::from);
    let metadata_existed = metadata.as_ref().is_ok_and(|path| path.exists());
    let metadata_owned = metadata.as_ref().is_ok_and(|path| path.starts_with(&work));
    let unused_absent = !root.path("unused").exists();
    job.cancel();
    let first = timeout(Duration::from_secs(3), job.finish())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(first.kind, ErrorKind::Cancelled);
    assert!(first.cleanup_issue.is_none());
    let pid = pid.unwrap();
    assert!(
        pid.trim().parse::<u32>().is_ok(),
        "ready PID must be complete"
    );
    let metadata = metadata.unwrap();
    assert!(metadata_existed);
    assert!(metadata_owned);
    assert!(unused_absent);
    assert!(!metadata.exists());
    assert!(fs::read_dir(&work).unwrap().next().is_none());
    assert_eq!(job.finish().await.unwrap_err(), first);
    assert_eq!(fs::read_to_string(root.path("spawns")).unwrap(), "spawn\n");
    assert!(
        !Command::new("/bin/kill")
            .args(["-0", "--", pid.trim()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    assert!(!root.path("final.mp4").exists());
}
#[cfg(unix)]
#[tokio::test]
async fn replaced_metadata_symlink_is_retained_as_unknown_without_following_cleanup() {
    use std::os::unix::fs::symlink;
    let root = Scratch::new();
    let work = root.work();
    let helper = helper(&root);
    fs::write(root.path("source.mkv"), b"owned process-lifetime fixture").unwrap();
    let mut media = SystemMedia::new(&helper, &tool("ffprobe"), &work, Bounds::default()).unwrap();
    let stage = ExportStage::new(&root.0, "partial.mp4", "final.mp4").unwrap();
    let request = trim::encode_command(
        &root.path("source.mkv"),
        stage.partial_path(),
        &root.path("unused"),
        EncodePlan {
            keep: &[Span {
                start: 0.0,
                end: 1.0,
            }],
            audio_streams: 0,
            chapters: &[],
            duration: 1.0,
        },
        "libx264",
    )
    .unwrap();
    let mut job = media.encode(&request, Cancellation::default()).unwrap();
    let readiness = wait_for_job_readiness(job.as_mut(), &root.path("metadata-path")).await;
    if readiness.is_err() {
        job.cancel();
        let joined = job.finish().await;
        panic!("metadata readiness failed after joined cleanup: {readiness:?}; joined: {joined:?}");
    }
    let metadata = fs::read_to_string(root.path("metadata-path")).map(PathBuf::from);
    if metadata.is_err() {
        job.cancel();
        let joined = job.finish().await;
        panic!(
            "published metadata path could not be read after joined cleanup: {metadata:?}; joined: {joined:?}"
        );
    }
    let metadata = metadata.unwrap();
    let original = root.path("owned-original-metadata");
    fs::rename(&metadata, &original).unwrap();
    let other = root.path("unrelated-owned-fixture");
    fs::write(&other, b"must survive").unwrap();
    symlink(&other, &metadata).unwrap();
    job.cancel();
    let error = job.finish().await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert!(error.cleanup_issue.is_some());
    assert_eq!(fs::read(&other).unwrap(), b"must survive");
    assert!(
        fs::symlink_metadata(&metadata)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(original.exists());
    assert!(!root.path("final.mp4").exists());
}
#[test]
fn private_work_root_bounds_and_preview_input_authority_are_explicit() {
    let root = Scratch::new();
    assert!(
        SystemMedia::new(
            &tool("ffmpeg"),
            &tool("ffprobe"),
            &root.path("not-created"),
            Bounds::default()
        )
        .is_err()
    );
    let work = root.work();
    let invalid = Bounds {
        encode_timeout: Duration::ZERO,
        ..Bounds::default()
    };
    assert!(SystemMedia::new(&tool("ffmpeg"), &tool("ffprobe"), &work, invalid).is_err());
    let mut media = backend(&work);
    let request = SheetRequest {
        path: root.path("final.mp4"),
        times: vec![0.5],
        columns: 1,
        width: 80,
        max_image_bytes: 1024,
    };
    assert_eq!(
        media
            .preview(&request, Cancellation::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsafePath
    );
    assert!(fs::read_dir(&work).unwrap().next().is_none());
}

#[tokio::test]
async fn installed_ffmpeg_contact_sheet_matches_source_pixels_dimensions_and_cached_finish() {
    let root = Scratch::new();
    let work = root.work();
    let source = generate(&root).await;
    let request = SheetRequest {
        path: source.clone(),
        times: vec![0.5, 2.5, 1.5],
        columns: 2,
        width: 80,
        max_image_bytes: 1024 * 1024,
    };
    let mut media = backend(&work);
    let mut job = media.preview(&request, Cancellation::default()).unwrap();
    let jpeg = job.finish().await.unwrap();
    assert!(Arc::ptr_eq(&jpeg, &job.finish().await.unwrap()));
    assert!(fs::read_dir(&work).unwrap().next().is_none());
    let image = root.path("returned.jpg");
    fs::write(&image, &jpeg).unwrap();
    let mut decode_args = args(&["-v", "error", "-i"]);
    decode_args.push(image.as_os_str().to_owned());
    decode_args.extend(args(&[
        "-frames:v",
        "1",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "rgb24",
        "pipe:1",
    ]));
    let rgb = capture_ffmpeg(&decode_args, 164 * 96 * 3).await;
    assert_eq!(rgb.len(), 164 * 96 * 3);
    let pixel = |x: usize, y: usize| &rgb[(y * 164 + x) * 3..(y * 164 + x) * 3 + 3];
    assert!(pixel(40, 23)[0] > 220 && pixel(40, 23)[2] < 30);
    assert!(pixel(124, 23)[2] > 220 && pixel(124, 23)[0] < 30);
    assert!(pixel(40, 73)[0] > 220 && pixel(40, 73)[2] < 30);
    assert!(pixel(124, 73).iter().all(|channel| *channel > 220));
    let oracle = Command::new("node")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap())
        .args(["--input-type=module", "-e", "import {contactSheet} from './build/tools/trim.js';process.stdout.write(await contactSheet(process.argv[1],[0.5,2.5,1.5],2,80));"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        oracle.status.success(),
        "source oracle failed: {}",
        String::from_utf8_lossy(&oracle.stderr)
    );
    assert_eq!(&*jpeg, oracle.stdout);
}

#[tokio::test]
async fn source_default_sheet_layout_is_concrete_and_byte_limits_fail_without_overwrite() {
    let root = Scratch::new();
    let work = root.work();
    let source = generate(&root).await;
    let times = trim::contact_sheet_times(None, &[], 4.0);
    assert_eq!(times, vec![0.25, 0.75, 1.25, 1.75, 2.25, 2.75, 3.25, 3.75]);
    let mut media = backend(&work);
    let mut request = SheetRequest {
        path: source.clone(),
        times,
        columns: 4,
        width: 320,
        max_image_bytes: 1024 * 1024,
    };
    let image = media
        .preview(&request, Cancellation::default())
        .unwrap()
        .finish()
        .await
        .unwrap();
    let oracle = Command::new("node")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap())
        .args(["--input-type=module", "-e", "import {contactSheet} from './build/tools/trim.js';process.stdout.write(await contactSheet(process.argv[1],Array.from({length:8},(_,i)=>(i+.5)*4/8),4,320));"])
        .arg(&source).output().unwrap();
    assert!(oracle.status.success());
    assert_eq!(&*image, oracle.stdout);
    let prior = root.path("sheet.jpg");
    fs::write(&prior, b"preserve preexisting output").unwrap();
    request.max_image_bytes = 16;
    let mut job = media.preview(&request, Cancellation::default()).unwrap();
    let error = job.finish().await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Preview);
    assert!(error.message().contains("byte limit"));
    assert_eq!(job.finish().await.unwrap_err(), error);
    assert_eq!(fs::read(&prior).unwrap(), b"preserve preexisting output");
    assert!(fs::read_dir(&work).unwrap().next().is_none());
    request.max_image_bytes = 0;
    assert_eq!(
        media
            .preview(&request, Cancellation::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Limit
    );
    request.max_image_bytes = 1024 * 1024;
    request.times = vec![0.0; 25];
    assert_eq!(
        media
            .preview(&request, Cancellation::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Evidence
    );
    request.times = vec![f64::NAN];
    assert!(media.preview(&request, Cancellation::default()).is_err());
    let cancelled = Cancellation::default();
    cancelled.cancel();
    assert_eq!(
        media.preview(&request, cancelled).err().unwrap().kind,
        ErrorKind::Cancelled
    );
    assert!(fs::read_dir(&work).unwrap().next().is_none());
}

#[tokio::test]
async fn preview_refuses_unowned_frame_collisions_and_invalid_input_without_erasing_old_files() {
    let root = Scratch::new();
    let work = root.work();
    let source = generate(&root).await;
    let mut media = backend(&work);
    let mut request = SheetRequest {
        path: source,
        times: vec![0.5],
        columns: 1,
        width: 80,
        max_image_bytes: 1024 * 1024,
    };
    let mut job = media.preview(&request, Cancellation::default()).unwrap();
    let directory = fs::read_dir(&work).unwrap().next().unwrap().unwrap().path();
    let unowned = directory.join("000.jpg");
    fs::write(&unowned, b"preexisting frame must survive").unwrap();
    let error = job.finish().await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::UnsafePath);
    assert!(error.cleanup_issue.is_some());
    assert_eq!(
        fs::read(&unowned).unwrap(),
        b"preexisting frame must survive"
    );
    drop(job);
    assert!(unowned.exists());
    // Remove only this task-owned collision fixture before checking ordinary
    // malformed-media cleanup in a fresh private child directory.
    fs::remove_file(&unowned).unwrap();
    fs::remove_dir(&directory).unwrap();
    request.path = root.path("malformed.mkv");
    fs::write(&request.path, b"not a media container").unwrap();
    let old_output = root.path("sheet.jpg");
    fs::write(&old_output, b"old output").unwrap();
    let error = media
        .preview(&request, Cancellation::default())
        .unwrap()
        .finish()
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Preview);
    assert!(error.cleanup_issue.is_none());
    assert_eq!(fs::read(&old_output).unwrap(), b"old output");
    assert!(fs::read_dir(&work).unwrap().next().is_none());
}

#[tokio::test]
async fn contact_sheet_pixel_budget_refuses_oversized_tile_before_publishing_frames() {
    let root = Scratch::new();
    let work = root.work();
    let source = root.path("tall.mkv");
    let mut make = args(&[
        "-v",
        "error",
        "-f",
        "lavfi",
        "-i",
        "color=c=red:s=80x800:r=1:d=1",
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
    ]);
    make.push(source.as_os_str().to_owned());
    run_ffmpeg(&make).await;
    let mut media = backend(&work);
    let mut request = SheetRequest {
        path: source,
        times: vec![0.0; 24],
        columns: 8,
        width: 640,
        max_image_bytes: 1024 * 1024,
    };
    let error = media
        .preview(&request, Cancellation::default())
        .unwrap()
        .finish()
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Limit);
    assert!(error.message().contains("pixel capacity"));
    assert!(fs::read_dir(&work).unwrap().next().is_none());
    request.max_image_bytes = 32 * 1024 * 1024 + 1;
    assert_eq!(
        media
            .preview(&request, Cancellation::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Limit
    );
    assert!(fs::read_dir(&work).unwrap().next().is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn abandoned_preview_await_retains_resistant_child_and_frames_until_join_and_refuses_replaced_cleanup()
 {
    use std::os::unix::fs::{PermissionsExt, symlink};
    for replace in [false, true] {
        let root = Scratch::new();
        let work = root.work();
        let source = generate(&root).await;
        let frame = root.path("fixture.jpg");
        let mut make = args(&["-v", "error", "-ss", "0.5", "-i"]);
        make.push(source.as_os_str().to_owned());
        make.extend(args(&["-vf", "scale=80:-2", "-frames:v", "1", "-q:v", "4"]));
        make.push(frame.as_os_str().to_owned());
        run_ffmpeg(&make).await;
        let helper = root.path("preview-fixture.cjs");
        fs::write(&helper,r#"#!/usr/bin/env node
const fs=require('node:fs'),p=require('node:path'),root=p.dirname(process.argv[1]),args=process.argv.slice(2);
fs.appendFileSync(p.join(root,'spawns'),'spawn\n');
if(!args.includes('-framerate')){process.stdout.write(fs.readFileSync(p.join(root,'fixture.jpg')))}
else{process.on('SIGTERM',()=>{});fs.writeFileSync(p.join(root,'pattern'),args[args.indexOf('-i')+1]);fs.writeFileSync(p.join(root,'ready'),String(process.pid));setInterval(()=>{},1000)}
"#).unwrap();
        fs::set_permissions(&helper, fs::Permissions::from_mode(0o700)).unwrap();
        let mut media =
            SystemMedia::new(&helper, &tool("ffprobe"), &work, Bounds::default()).unwrap();
        let request = SheetRequest {
            path: source,
            times: vec![0.5],
            columns: 1,
            width: 80,
            max_image_bytes: 1024 * 1024,
        };
        let mut job = media.preview(&request, Cancellation::default()).unwrap();
        let readiness = wait_for_job_readiness(job.as_mut(), &root.path("ready")).await;
        if readiness.is_err() {
            job.cancel();
            let joined = job.finish().await;
            panic!(
                "resistant preview readiness failed after joined cleanup: {readiness:?}; joined: {joined:?}"
            );
        }
        let pid = fs::read_to_string(root.path("ready"));
        let pattern = fs::read_to_string(root.path("pattern"));
        if pid.is_err() || pattern.is_err() {
            job.cancel();
            let joined = job.finish().await;
            panic!(
                "resistant preview evidence unavailable after joined cleanup: pid={pid:?}, pattern={pattern:?}; joined: {joined:?}"
            );
        }
        let pid = pid.unwrap();
        let pattern = pattern.unwrap();
        let owned = PathBuf::from(pattern.replace("%03d", "000"));
        let frame_retained = owned.exists();
        let original = root.path("original.jpg");
        let other = root.path("unrelated.jpg");
        fs::write(&other, b"must survive").unwrap();
        if replace {
            fs::rename(&owned, &original).unwrap();
            symlink(&other, &owned).unwrap();
        }
        job.cancel();
        let error = timeout(Duration::from_secs(3), job.finish())
            .await
            .unwrap()
            .unwrap_err();
        assert!(frame_retained);
        assert_eq!(error.kind, ErrorKind::Cancelled);
        assert_eq!(error.cleanup_issue.is_some(), replace);
        assert_eq!(job.finish().await.unwrap_err(), error);
        assert_eq!(
            fs::read_to_string(root.path("spawns")).unwrap(),
            "spawn\nspawn\n"
        );
        assert_eq!(fs::read(&other).unwrap(), b"must survive");
        assert!(
            !Command::new("/bin/kill")
                .args(["-0", "--", pid.trim()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap()
                .success()
        );
        if replace {
            assert!(
                fs::symlink_metadata(&owned)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            assert!(original.exists());
        } else {
            assert!(fs::read_dir(&work).unwrap().next().is_none());
        }
    }
}
#[cfg(unix)]
#[tokio::test]
async fn preview_phase_timeout_before_helper_ready_caches_failure_and_joins_work_cleanup() {
    use std::{future::poll_fn, os::unix::fs::PermissionsExt, task::Poll};
    let root = Scratch::new();
    let work = root.work();
    let source = root.path("source.mkv");
    fs::write(&source, b"owned lifecycle fixture").unwrap();
    let helper = root.path("timeout-preview.cjs");
    fs::write(&helper,r#"#!/usr/bin/env node
const fs=require('node:fs'),p=require('node:path'),root=p.dirname(process.argv[1]);
// Deliberately withhold initialization markers until the test grants readiness.
// The closed gate remains deterministic even when Node starts instantly.
const waiting=setInterval(()=>{if(!fs.existsSync(p.join(root,'publish-ready')))return;clearInterval(waiting);process.on('SIGTERM',()=>{});fs.writeFileSync(p.join(root,'spawns.tmp'),'spawn\n');fs.renameSync(p.join(root,'spawns.tmp'),p.join(root,'spawns'));fs.writeFileSync(p.join(root,'ready.tmp'),String(process.pid));fs.renameSync(p.join(root,'ready.tmp'),p.join(root,'ready'));setInterval(()=>{},1000)},10);
"#).unwrap();
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o700)).unwrap();
    let mut media = SystemMedia::new(
        &helper,
        &tool("ffprobe"),
        &work,
        Bounds {
            sheet_command_timeout: Duration::from_secs(1),
            ..Bounds::default()
        },
    )
    .unwrap();
    let request = SheetRequest {
        path: source,
        times: vec![0.5],
        columns: 1,
        width: 80,
        max_image_bytes: 1024 * 1024,
    };
    let mut job = media.preview(&request, Cancellation::default()).unwrap();
    // This case intentionally covers the deadline before helper readiness.
    // A phase timeout proves neither that Node initialized nor SIGTERM
    // resistance. The separate readiness-gated cancellation test above proves
    // those properties and checks disappearance of its exact published PID.
    let completed = timeout(Duration::from_secs(3), job.finish()).await;
    let within_join_budget = completed.is_ok();
    let result = match completed {
        Ok(result) => result,
        Err(_) => {
            // Drain owned cleanup before any assertion can unwind the fixture.
            job.cancel();
            job.finish().await
        }
    };
    // Cached completion must be Ready on one poll, not another process or
    // another phase timeout hidden behind identical error text.
    let cached_poll = {
        let mut cached = job.finish();
        poll_fn(|cx| Poll::Ready(cached.as_mut().poll(cx))).await
    };
    let immediately_cached = cached_poll.is_ready();
    let cached = match cached_poll {
        Poll::Ready(result) => result,
        Poll::Pending => {
            // The temporary finish borrow is gone; settle the owner before
            // reporting a cache/non-replay regression.
            job.cancel();
            job.finish().await
        }
    };
    assert!(
        immediately_cached,
        "completed preview must return its cached result on the first poll"
    );
    assert!(
        within_join_budget,
        "pre-readiness timeout cleanup exceeded the 3s join budget"
    );
    let error = result.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Preview);
    assert!(error.message().contains("timed out"));
    assert!(error.cleanup_issue.is_none());
    assert_eq!(cached.unwrap_err(), error);
    assert!(fs::read_dir(&work).unwrap().next().is_none());
    assert!(
        !root.path("publish-ready").exists(),
        "readiness gate was never granted"
    );
    assert!(
        !root.path("ready").exists(),
        "deadline must not imply helper readiness"
    );
    assert!(
        !root.path("spawns").exists(),
        "withheld helper markers must remain absent"
    );
}
struct LoopbackCounter {
    address: std::net::SocketAddr,
    count: Arc<AtomicUsize>,
    task: Option<JoinHandle<()>>,
}
impl LoopbackCounter {
    async fn new(segment: PathBuf) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                observed.fetch_add(1, Ordering::SeqCst);
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).await;
                let mut file = fs::File::open(&segment).unwrap();
                let length = file.metadata().unwrap().len();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: video/mp2t\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n"
                );
                if socket.write_all(header.as_bytes()).await.is_ok() {
                    let mut buffer = [0u8; 8192];
                    while let Ok(count) = file.read(&mut buffer) {
                        if count == 0 || socket.write_all(&buffer[..count]).await.is_err() {
                            break;
                        }
                    }
                }
                let _ = socket.shutdown().await;
            }
        });
        Self {
            address,
            count,
            task: Some(task),
        }
    }
    async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}
impl Drop for LoopbackCounter {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
#[tokio::test]
async fn local_hls_and_concat_are_blocked_in_probe_decode_encode_and_preview_before_secondary_io() {
    let root = Scratch::new();
    let work = root.work();
    let source = generate(&root).await;
    let manifest = root.path("playlist.m3u8");
    let mut make = args(&["-v", "error", "-i"]);
    make.push(source.as_os_str().to_owned());
    make.extend(args(&[
        "-map",
        "0:v:0",
        "-map",
        "0:a:0",
        "-c",
        "copy",
        "-f",
        "hls",
        "-hls_time",
        "1",
        "-hls_list_size",
        "0",
    ]));
    make.push(manifest.as_os_str().to_owned());
    run_ffmpeg(&make).await;
    let segment = fs::read_dir(&root.0)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "ts"))
        .unwrap();
    let mut http = LoopbackCounter::new(segment).await;
    let original = fs::read_to_string(&manifest).unwrap();
    let remote = original
        .lines()
        .map(|line| {
            if line.is_empty() || line.starts_with('#') {
                line.to_owned()
            } else {
                format!("http://{}/segment.ts", http.address)
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&manifest, format!("{remote}\n")).unwrap();
    // Positive fixture control: a deliberately permissive non-product probe can
    // fetch the owned HTTP segment. The product must forbid both that protocol and
    // the playlist demuxer, even when a file is inside the approved scratch root.
    let mut permissive = args(&[
        "-v",
        "error",
        "-protocol_whitelist",
        "file,http,tcp",
        "-show_format",
    ]);
    permissive.push(manifest.as_os_str().to_owned());
    let mut control = media_verification::capture_bounded_media_process(
        tool("ffprobe"),
        &permissive,
        ProcessOptions {
            timeout: Duration::from_secs(5),
            ..ProcessOptions::default()
        },
        64 * 1024,
    )
    .unwrap();
    let _ = control.finish().await;
    assert!(
        http.count.load(Ordering::SeqCst) > 0,
        "HLS fixture did not exercise secondary network input"
    );
    http.count.store(0, Ordering::SeqCst);
    let disguised = root.path("looks-like-recording.mp4");
    fs::copy(&manifest, &disguised).unwrap();
    let concat = root.path("external.concat");
    fs::write(
        &concat,
        format!("ffconcat version 1.0\nfile '{}'/\n", source.display()).replace("'/\n", "'\n"),
    )
    .unwrap();
    let mut media = backend(&work);
    let input = probe(&mut media, &source).await;
    let encoders = encoder_list(&media).await;
    for (path, label) in [
        (&manifest, "hls"),
        (&disguised, "disguised-hls"),
        (&concat, "concat"),
    ] {
        let error = media
            .probe(path, Cancellation::default())
            .unwrap()
            .finish()
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Probe);
        if label != "disguised-hls" {
            assert!(error.message().contains("whitelist"), "{label}: {error}");
        }
        let error = media
            .decode(path, Cancellation::default())
            .unwrap()
            .finish()
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Decode);
        if label != "disguised-hls" {
            assert!(error.message().contains("whitelist"));
        }
        let sheet = SheetRequest {
            path: path.to_owned(),
            times: vec![0.5],
            columns: 1,
            width: 80,
            max_image_bytes: 1024 * 1024,
        };
        let error = media
            .preview(&sheet, Cancellation::default())
            .unwrap()
            .finish()
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Preview);
        assert!(fs::read_dir(&work).unwrap().next().is_none());
        let stage = ExportStage::new(
            &root.0,
            &format!("{label}.partial.mp4"),
            &format!("{label}.final.mp4"),
        )
        .unwrap();
        let request = trim::encode_command(
            path,
            stage.partial_path(),
            &root.path("unused-metadata"),
            EncodePlan {
                keep: &[Span {
                    start: 0.0,
                    end: 4.0,
                }],
                audio_streams: input.audio_streams,
                chapters: &[],
                duration: 4.0,
            },
            &encoders,
        )
        .unwrap();
        let mut export = ExportOwner::new(
            backend(&work),
            stage,
            request,
            &input,
            ExpectedMetadata {
                duration: 4.0,
                width: input.width,
                height: input.height,
                audio_streams: input.audio_streams,
                chapters: vec![],
            },
            None,
            Cancellation::default(),
        )
        .unwrap();
        let error = export.finish().await.err().unwrap();
        assert_eq!(error.kind, ErrorKind::Encode);
        assert!(!root.path(&format!("{label}.final.mp4")).exists());
        assert!(!root.path(&format!("{label}.partial.mp4")).exists());
        assert!(fs::read_dir(&work).unwrap().next().is_none());
    }
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(
        http.count.load(Ordering::SeqCst),
        0,
        "a closed media input made a network connection"
    );
    http.stop().await;
}
#[tokio::test]
async fn ordinary_mov_and_mp4_inputs_stay_qualified_without_extra_codec_constraints() {
    let root = Scratch::new();
    let work = root.work();
    let source = generate(&root).await;
    let mut media = backend(&work);
    for extension in ["mov", "mp4"] {
        let path = root.path(&format!("ordinary.{extension}"));
        let mut remux = args(&["-v", "error", "-i"]);
        remux.push(source.as_os_str().to_owned());
        remux.extend(args(&["-map", "0:v:0", "-map", "0:a:0", "-c", "copy"]));
        remux.push(path.as_os_str().to_owned());
        run_ffmpeg(&remux).await;
        let info = probe(&mut media, &path).await;
        assert_eq!(info.video_streams, 1);
        assert_eq!(info.audio_streams, 1);
        media
            .decode(&path, Cancellation::default())
            .unwrap()
            .finish()
            .await
            .unwrap();
    }
    assert!(fs::read_dir(&work).unwrap().next().is_none());
}

// Ready is a publication boundary for both the PID and complete metadata path.
// A controlled producer gate makes an early ready marker reproducible without
// relying on scheduler timing. Always join the child before asserting failure.
#[cfg(unix)]
#[tokio::test]
async fn helper_ready_is_not_visible_before_complete_metadata_publication() {
    let root = Scratch::new();
    let work = root.work();
    let helper = helper(&root);
    fs::write(root.path("hold-metadata"), b"hold").unwrap();
    fs::write(root.path("source.mkv"), b"owned readiness fixture").unwrap();
    let mut media = SystemMedia::new(&helper, &tool("ffprobe"), &work, Bounds::default()).unwrap();
    let stage = ExportStage::new(&root.0, "partial.mp4", "final.mp4").unwrap();
    let request = trim::encode_command(
        &root.path("source.mkv"),
        stage.partial_path(),
        &root.path("unused"),
        EncodePlan {
            keep: &[Span {
                start: 0.0,
                end: 1.0,
            }],
            audio_streams: 0,
            chapters: &[],
            duration: 1.0,
        },
        "libx264",
    )
    .unwrap();
    let mut job = media.encode(&request, Cancellation::default()).unwrap();
    let readiness = wait_for_job_readiness(job.as_mut(), &root.path("metadata-paused")).await;
    if readiness.is_err() {
        job.cancel();
        let joined = job.finish().await;
        panic!(
            "metadata publication gate was not reached before joined cleanup: {readiness:?}; joined: {joined:?}"
        );
    }
    let premature_ready = root.path("ready").exists();
    fs::remove_file(root.path("hold-metadata")).unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    let (pid, metadata) = loop {
        let pid = fs::read_to_string(root.path("ready")).unwrap_or_default();
        let metadata = fs::read_to_string(root.path("metadata-path")).unwrap_or_default();
        if pid.parse::<u32>().is_ok() && !metadata.is_empty() {
            break (pid, PathBuf::from(metadata));
        }
        if Instant::now() >= until {
            job.cancel();
            let joined = job.finish().await;
            panic!("complete ready fixture did not publish; owned cleanup joined: {joined:?}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    let metadata_was_owned = metadata.starts_with(&work) && metadata.exists();
    job.cancel();
    let result = timeout(Duration::from_secs(3), job.finish())
        .await
        .unwrap()
        .unwrap_err();
    let child_absent = !Command::new("/bin/kill")
        .args(["-0", "--", pid.trim()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success();
    assert_eq!(result.kind, ErrorKind::Cancelled);
    assert!(result.cleanup_issue.is_none());
    assert!(
        child_absent,
        "owned readiness child must be reaped before fixture assertions"
    );
    assert!(metadata_was_owned);
    assert!(!metadata.exists());
    assert_eq!(fs::read_to_string(root.path("spawns")).unwrap(), "spawn\n");
    assert!(
        !premature_ready,
        "ready became visible before the metadata-path publication boundary"
    );
}
