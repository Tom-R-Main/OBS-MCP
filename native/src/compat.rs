// SPDX-License-Identifier: GPL-2.0-only
use std::{io, path::PathBuf, process::Stdio, time::Duration};

use serde_json::Value;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Retain partial frames in the owner so cancelling `next` never loses input.
pub struct Frames<R> {
    reader: R,
    pending: Vec<u8>,
}

impl<R: AsyncBufRead + Unpin> Frames<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            pending: Vec::new(),
        }
    }

    pub async fn next(&mut self) -> io::Result<Option<Vec<u8>>> {
        loop {
            let available = self.reader.fill_buf().await?;
            if available.is_empty() {
                return if self.pending.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(std::mem::take(&mut self.pending)))
                };
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let count = newline.map_or(available.len(), |offset| offset + 1);
            if self.pending.len() + count > MAX_FRAME_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "MCP frame exceeds 8 MiB",
                ));
            }
            self.pending.extend_from_slice(&available[..count]);
            self.reader.consume(count);
            if newline.is_some() {
                return Ok(Some(std::mem::take(&mut self.pending)));
            }
        }
    }
}

pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, frame: &[u8]) -> io::Result<()> {
    writer.write_all(frame).await?;
    if !frame.ends_with(b"\n") {
        writer.write_all(b"\n").await?;
    }
    writer.flush().await
}

fn entry() -> io::Result<PathBuf> {
    if let Some(configured) = std::env::var_os("OBS_MCP_COMPAT_ENTRY") {
        let path = PathBuf::from(configured);
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "OBS_MCP_COMPAT_ENTRY must be absolute",
            ));
        }
        return Ok(path);
    }
    let local = std::env::current_dir()?.join("build/index.js");
    if local.is_file() {
        return Ok(local);
    }
    let executable = std::env::current_exe()?;
    let packaged = executable
        .parent()
        .ok_or_else(|| io::Error::other("Native executable has no parent"))?
        .join("server/index.js");
    if packaged.is_file() {
        return Ok(packaged);
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "Compatibility server missing; set OBS_MCP_COMPAT_ENTRY to its absolute index.js path",
    ))
}

/// HTTP and explicit CLI modes retain the original server's process/stdio semantics.
#[cfg(unix)]
pub fn exec() -> io::Result<()> {
    use std::os::unix::process::CommandExt;
    let node = std::env::var_os("OBS_MCP_COMPAT_NODE").unwrap_or_else(|| "node".into());
    Err(std::process::Command::new(node)
        .arg(entry()?)
        .args(std::env::args_os().skip(1))
        .exec())
}

#[cfg(not(unix))]
pub fn exec() -> io::Result<()> {
    let node = std::env::var_os("OBS_MCP_COMPAT_NODE").unwrap_or_else(|| "node".into());
    let status = std::process::Command::new(node)
        .arg(entry()?)
        .args(std::env::args_os().skip(1))
        .status()?;
    std::process::exit(status.code().unwrap_or(1));
}

struct Compatibility {
    child: Child,
    input: Option<ChildStdin>,
    output: Frames<BufReader<ChildStdout>>,
    #[cfg(unix)]
    group: u32,
}

impl Compatibility {
    async fn spawn() -> io::Result<Self> {
        let node = std::env::var_os("OBS_MCP_COMPAT_NODE").unwrap_or_else(|| "node".into());
        let mut command = Command::new(node);
        command.arg(entry()?).args(std::env::args_os().skip(1));
        Self::from_command(command)
    }

    fn from_command(mut command: Command) -> io::Result<Self> {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.as_std_mut().process_group(0);
        }
        let mut child = command.spawn()?;
        #[cfg(unix)]
        let group = child
            .id()
            .ok_or_else(|| io::Error::other("Compatibility child has no PID"))?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("Compatibility stdin missing"))?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("Compatibility stdout missing"))?;
        Ok(Self {
            child,
            input: Some(input),
            output: Frames::new(BufReader::new(output)),
            #[cfg(unix)]
            group,
        })
    }

    async fn send(&mut self, frame: &[u8]) -> io::Result<()> {
        write_frame(
            self.input
                .as_mut()
                .ok_or_else(|| io::Error::other("Compatibility stdin closed"))?,
            frame,
        )
        .await
    }

    async fn replay(&mut self, history: &[Vec<u8>]) -> io::Result<()> {
        for frame in history {
            self.send(frame).await?;
            let message: Value = serde_json::from_slice(frame).map_err(io::Error::other)?;
            if message.get("method").and_then(Value::as_str) == Some("initialize") {
                let expected_id = message.get("id");
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let response = self.output.next().await?.ok_or_else(|| {
                            io::Error::other("Compatibility child closed during initialize replay")
                        })?;
                        let value: Value =
                            serde_json::from_slice(&response).map_err(io::Error::other)?;
                        if value.get("id") == expected_id {
                            return Ok::<(), io::Error>(());
                        }
                    }
                })
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Compatibility initialize replay timed out",
                    )
                })??;
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn signal_group(&self, signal: &str) -> io::Result<()> {
        // The child has NOT been waited/reaped: its PID still reserves this owned PGID.
        let mut signaler = Command::new("/bin/kill");
        signaler
            .arg(signal)
            .arg("--")
            .arg(format!("-{}", self.group))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let status = tokio::time::timeout(Duration::from_secs(1), signaler.status())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Compatibility group signal timed out",
                )
            })??;
        if !status.success() {
            // A gone group is already clean. Keep the unreaped leader until all signals finish.
            let gone = Command::new("/bin/kill")
                .arg("-0")
                .arg("--")
                .arg(format!("-{}", self.group))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await?;
            if gone.success() {
                return Err(io::Error::other(
                    "Could not signal compatibility process group",
                ));
            }
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.input.take(); // EOF requests normal SDK shutdown first.
        let _ = tokio::time::timeout(Duration::from_millis(1500), async {
            while self.output.next().await?.is_some() {}
            Ok::<(), io::Error>(())
        })
        .await;
        #[cfg(unix)]
        {
            // Do not reap the leader early: a grandchild may retain pipes after Node exits.
            let term = self.signal_group("-TERM").await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            let kill = self.signal_group("-KILL").await;
            let waited = tokio::time::timeout(Duration::from_secs(2), self.child.wait())
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Compatibility child exit unconfirmed",
                    )
                })?;
            waited?;
            term?;
            kill?;
        }
        #[cfg(not(unix))]
        {
            self.child.start_kill()?;
            self.child.wait().await?;
        }
        Ok(())
    }
}

/// A one-way handoff: this child exclusively owns every subsequent MCP message.
pub async fn run<R, W, F>(
    input: &mut Frames<R>,
    output: &mut W,
    history: &[Vec<u8>],
    initial: Vec<Vec<u8>>,
    mut stop: std::pin::Pin<&mut F>,
) -> io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    F: std::future::Future<Output = ()>,
{
    let mut compatibility = Compatibility::spawn().await?;
    let result = async {
        compatibility.replay(history).await?;
        for frame in initial {
            compatibility.send(&frame).await?;
        }
        loop {
            tokio::select! {
                biased;
                () = &mut stop => break,
                frame = input.next() => match frame? {
                    Some(frame) => compatibility.send(&frame).await?,
                    None => break,
                },
                frame = compatibility.output.next() => match frame? {
                    Some(frame) => write_frame(output, &frame).await?,
                    None => break,
                },
            }
        }
        Ok(())
    }
    .await;
    let cleanup = compatibility.shutdown().await;
    result.and(cleanup)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frame_cancellation_preserves_partial_bytes() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let mut frames = Frames::new(BufReader::new(reader));
        writer.write_all(b"{\"id\":").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), frames.next())
                .await
                .is_err()
        );
        writer.write_all(b"1}\n{\"id\":2}\n").await.unwrap();
        assert_eq!(frames.next().await.unwrap().unwrap(), b"{\"id\":1}\n");
        assert_eq!(frames.next().await.unwrap().unwrap(), b"{\"id\":2}\n");
    }

    #[tokio::test]
    async fn rejects_oversize_frames_and_preserves_final_unterminated_frame() {
        let mut oversized = Frames::new(BufReader::new(std::io::Cursor::new(vec![
            b'x';
            MAX_FRAME_BYTES
                + 1
        ])));
        assert_eq!(
            oversized.next().await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        let mut final_frame = Frames::new(BufReader::new(std::io::Cursor::new(b"{}")));
        assert_eq!(final_frame.next().await.unwrap().unwrap(), b"{}");
        assert!(final_frame.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn legacy_replay_hides_its_reply_and_keeps_one_child_for_later_requests() {
        let mut command = Command::new("node");
        command.arg("-e").arg("let initialized=false,count=0; require('readline').createInterface({input:process.stdin}).on('line',line=>{const m=JSON.parse(line);if(m.method==='notifications/initialized'){initialized=true;return;}console.log(JSON.stringify({jsonrpc:'2.0',id:m.id,result:{pid:process.pid,initialized,count:++count}}));});");
        let mut child = Compatibility::from_command(command).unwrap();
        let result = async {
            child.replay(&[
                br#"{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#.to_vec(),
                br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#.to_vec(),
            ]).await?;
            child.send(br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"mutating-tool"}}"#).await?;
            let first: Value = serde_json::from_slice(&child.output.next().await?.unwrap()).map_err(io::Error::other)?;
            assert_eq!(first["id"], 1);
            assert_eq!(first["result"]["initialized"], true);
            child.send(br#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#).await?;
            let second: Value = serde_json::from_slice(&child.output.next().await?.unwrap()).map_err(io::Error::other)?;
            assert_eq!(second["id"], 2);
            assert_eq!(first["result"]["pid"], second["result"]["pid"]);
            assert_eq!(second["result"]["count"], 3);
            Ok::<(), io::Error>(())
        }.await;
        let cleanup = child.shutdown().await;
        result.unwrap();
        cleanup.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cleanup_kills_ignored_term_descendant_even_after_node_leader_exits() {
        let mut command = Command::new("node");
        command.arg("-e").arg("const {spawn}=require('child_process');spawn(process.execPath,['-e',\"process.on('SIGTERM',()=>{});console.log(process.pid);setInterval(()=>{},1000)\"],{stdio:['ignore','inherit','inherit']});process.stdin.resume();process.stdin.on('end',()=>process.exit(0));");
        let mut child = Compatibility::from_command(command).unwrap();
        let descendant = tokio::time::timeout(Duration::from_secs(3), child.output.next()).await;
        let cleanup = child.shutdown().await;
        let descendant: u32 = String::from_utf8(descendant.unwrap().unwrap().unwrap())
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        cleanup.unwrap();
        assert!(
            child.child.id().is_none(),
            "the owned leader must actually be waited/reaped"
        );
        // An orphaned zombie may await the system reaper in a container; it is not a live job.
        // Do not claim kill(pid,0) proves reaping of a process whose parent already exited.
        let state = std::process::Command::new("/bin/ps")
            .args(["-o", "stat=", "-p", &descendant.to_string()])
            .output()
            .unwrap();
        let state = String::from_utf8_lossy(&state.stdout);
        assert!(
            state.trim().is_empty() || state.trim().starts_with('Z'),
            "owned descendant remains alive: {state}"
        );
    }
}
