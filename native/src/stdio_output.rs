// SPDX-License-Identifier: GPL-2.0-only
//! One retained writer; input never waits for an unread output pipe.
use std::{io, io::Write, sync::Arc};

use serde_json::Value;
use tokio::{
    io::AsyncWrite,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinHandle,
};

const ITEMS: usize = 128;
const BYTES: usize = 32 * 1024 * 1024;

struct Frame {
    bytes: Vec<u8>,
    _item: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

pub(crate) struct Output {
    sender: mpsc::Sender<Frame>,
    items: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    limit: usize,
}

struct Count {
    length: usize,
    limit: usize,
}
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.length = self
            .length
            .checked_add(bytes.len())
            .filter(|length| *length <= self.limit)
            .ok_or_else(capacity)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn capacity() -> io::Error {
    io::Error::other("Native output admission exhausted; live state retained until owned shutdown")
}

impl Output {
    pub(crate) fn spawn<W: AsyncWrite + Unpin + Send + 'static>(
        writer: W,
    ) -> (Self, JoinHandle<io::Result<()>>) {
        Self::with_limits(writer, ITEMS, BYTES)
    }

    fn with_limits<W: AsyncWrite + Unpin + Send + 'static>(
        mut writer: W,
        items: usize,
        bytes: usize,
    ) -> (Self, JoinHandle<io::Result<()>>) {
        let (sender, mut receiver) = mpsc::channel::<Frame>(items);
        let output = Self {
            sender,
            items: Arc::new(Semaphore::new(items)),
            bytes: Arc::new(Semaphore::new(bytes)),
            limit: bytes,
        };
        let worker = tokio::spawn(async move {
            while let Some(frame) = receiver.recv().await {
                // Retain the frame and both permits through the actual flush.
                // A partial write must never be dropped and restarted.
                crate::compat::write_frame(&mut writer, &frame.bytes).await?;
                drop(frame);
            }
            Ok(())
        });
        (output, worker)
    }

    pub(crate) fn try_send(&self, value: &Value) -> io::Result<()> {
        let item = self
            .items
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())?;
        let mut count = Count {
            length: 0,
            limit: self.limit.saturating_sub(1),
        };
        serde_json::to_writer(&mut count, value).map_err(io::Error::other)?;
        let length = count.length + 1;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(u32::try_from(length).map_err(|_| capacity())?)
            .map_err(|_| capacity())?;
        let mut encoded = Vec::with_capacity(length);
        serde_json::to_writer(&mut encoded, value).map_err(io::Error::other)?;
        encoded.push(b'\n');
        debug_assert_eq!(encoded.len(), length);
        self.sender
            .try_send(Frame {
                bytes: encoded,
                _item: item,
                _bytes: bytes,
            })
            .map_err(|_| capacity())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn partial_write_keeps_byte_budget_and_preserves_both_frames() {
        let (writer, mut reader) = tokio::io::duplex(1);
        let first = json!({"id": 1, "result": "é\n"});
        let second = json!({"id": 2});
        let first_len = serde_json::to_vec(&first).unwrap().len() + 1;
        let second_len = serde_json::to_vec(&second).unwrap().len() + 1;
        let (output, worker) = Output::with_limits(writer, 8, first_len + second_len);
        output.try_send(&first).unwrap();
        let mut prefix = [0];
        reader.read_exact(&mut prefix).await.unwrap();
        // The channel slot has been taken, but the partially written frame
        // still consumes its bytes. Even a tiny third line cannot be admitted.
        assert_eq!(output.bytes.available_permits(), second_len);
        output.try_send(&second).unwrap();
        assert!(output.try_send(&Value::Null).is_err());
        let bytes = output.bytes.clone();
        let items = output.items.clone();
        drop(output);
        let mut result = prefix.to_vec();
        reader.read_to_end(&mut result).await.unwrap();
        worker.await.unwrap().unwrap();
        let lines = std::str::from_utf8(&result)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(lines, [first, second]);
        assert_eq!(bytes.available_permits(), first_len + second_len);
        assert_eq!(items.available_permits(), 8);
    }

    #[tokio::test]
    async fn active_frame_keeps_item_and_oversize_refusal_returns_capacity() {
        let (writer, mut reader) = tokio::io::duplex(1);
        let (output, worker) = Output::with_limits(writer, 1, 64);
        assert!(output.try_send(&json!("x".repeat(64))).is_err());
        assert_eq!(output.items.available_permits(), 1);
        assert_eq!(output.bytes.available_permits(), 64);
        output.try_send(&json!({"id": 1})).unwrap();
        reader.read_u8().await.unwrap();
        assert!(output.try_send(&Value::Null).is_err());
        let items = output.items.clone();
        let bytes = output.bytes.clone();
        drop(reader);
        assert!(worker.await.unwrap().is_err());
        assert_eq!(items.available_permits(), 1);
        assert_eq!(bytes.available_permits(), 64);
        assert!(output.try_send(&Value::Null).is_err());
        assert_eq!(items.available_permits(), 1);
        assert_eq!(bytes.available_permits(), 64);
    }

    #[tokio::test]
    async fn writer_failure_releases_active_and_queued_frames_without_replay() {
        let (writer, mut reader) = tokio::io::duplex(1);
        let (output, worker) = Output::with_limits(writer, 3, 128);
        for id in 1..=3 {
            output.try_send(&json!({"id": id})).unwrap();
        }
        reader.read_u8().await.unwrap();
        assert_eq!(output.items.available_permits(), 0);
        assert!(output.try_send(&Value::Null).is_err());
        drop(reader);
        assert!(worker.await.unwrap().is_err());
        assert_eq!(output.items.available_permits(), 3);
        assert_eq!(output.bytes.available_permits(), 128);
    }
}
