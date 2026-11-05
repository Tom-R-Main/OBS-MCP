// SPDX-License-Identifier: MIT
//! Exact prefix replay and one absolute deadline for each pending write/flush.
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::{Instant, Sleep},
};
pub(super) struct PrefixIo<T> {
    pub inner: T,
    prefix: Vec<u8>,
    at: usize,
    write_limit: Duration,
    write_timer: Option<Pin<Box<Sleep>>>,
}
impl<T> PrefixIo<T> {
    pub fn new(inner: T, prefix: Vec<u8>, write_limit: Duration) -> Self {
        Self {
            inner,
            prefix,
            at: 0,
            write_limit,
            write_timer: None,
        }
    }
    fn timer(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        let timer = self.write_timer.get_or_insert_with(|| {
            Box::pin(tokio::time::sleep_until(Instant::now() + self.write_limit))
        });
        if std::future::Future::poll(timer.as_mut(), cx).is_ready() {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Native HTTP writer deadline",
            ))
        } else {
            Ok(())
        }
    }
}
impl<T: AsyncRead + Unpin> AsyncRead for PrefixIo<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.at < this.prefix.len() {
            let count = out.remaining().min(this.prefix.len() - this.at);
            out.put_slice(&this.prefix[this.at..this.at + count]);
            this.at += count;
            if this.at == this.prefix.len() {
                this.prefix = Vec::new();
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, out)
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for PrefixIo<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Err(error) = this.timer(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.inner).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(error) = this.timer(cx) {
            return Poll::Ready(Err(error));
        }
        match Pin::new(&mut this.inner).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                this.write_timer = None;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Err(error) = this.timer(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.inner).poll_write_vectored(cx, bufs)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(error) = this.timer(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}
