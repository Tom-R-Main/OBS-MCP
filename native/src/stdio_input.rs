// SPDX-License-Identifier: MIT
//! Bounded cancellation-safe MCP stdin framing; no child-process ownership.
use std::io;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncWriteExt, BufReader};
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
}
