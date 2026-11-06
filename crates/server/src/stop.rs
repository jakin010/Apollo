//! Closing the gRPC port as the lifecycle stops. Once it is stopping, the
//! listener closes, so new connections are refused at once, and the open
//! ones are told to finish (GOAWAY). Dropping the server cuts the calls
//! still open, which [`serve_until_stopped`] does at the drain deadline.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};
use tonic::transport::server::{Connected, TcpConnectInfo, TcpIncoming};

use apollo_engine::{Lifecycle, Phase};

/// The connections `listener` accepts until `lc` is stopping. Then the
/// listener is dropped, and `closed` fires so the server tells the open
/// connections to finish. Each connection fails once `server`'s sender is
/// dropped.
pub(crate) fn incoming(
    listener: TcpListener,
    lc: Lifecycle,
    closed: oneshot::Sender<()>,
    server: watch::Receiver<()>,
) -> impl Stream<Item = io::Result<Connection>> {
    let state = (TcpIncoming::from(listener), lc, closed, server);
    // Not `take_until`, which would keep the listener open.
    futures::stream::unfold(state, |(mut listener, lc, closed, server)| async move {
        tokio::select! {
            biased;
            () = lc.reached(Phase::Stopping) => {
                drop(listener);
                let _ = closed.send(());
                None
            }
            conn = listener.next() => {
                let conn = conn?.map(|io| Connection::new(io, server.clone()));
                Some((conn, (listener, lc, closed, server)))
            }
        }
    })
}

/// An accepted connection, which fails once the server is dropped, so the
/// calls still open on it end too.
pub(crate) struct Connection {
    io: TcpStream,
    /// Ends when the server is dropped.
    dropped: Pin<Box<dyn Future<Output = ()> + Send>>,
    cut: bool,
}

impl Connection {
    fn new(io: TcpStream, mut server: watch::Receiver<()>) -> Self {
        Self {
            io,
            // Nothing is ever sent; it ends when the sender drops.
            dropped: Box::pin(async move { while server.changed().await.is_ok() {} }),
            cut: false,
        }
    }

    /// Whether the server is gone. While it isn't, `cx` is woken when it goes.
    fn poll_cut(&mut self, cx: &mut Context<'_>) -> bool {
        if !self.cut && self.dropped.as_mut().poll(cx).is_ready() {
            self.cut = true;
        }
        self.cut
    }
}

fn cut<T>() -> Poll<io::Result<T>> {
    Poll::Ready(Err(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "the server stopped",
    )))
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.poll_cut(cx) {
            return cut();
        }
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.poll_cut(cx) {
            return cut();
        }
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if self.poll_cut(cx) {
            return cut();
        }
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.poll_cut(cx) {
            return cut();
        }
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

impl Connected for Connection {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> TcpConnectInfo {
        self.io.connect_info()
    }
}

/// Run `serve` (a [`serve_on`](crate::serve_on) future) until it ends, or,
/// once `lc` is stopping, until the drain timeout passes or a forced stop.
/// Either of those drops `serve`, which cuts the calls still open.
/// `timeout` is read when the stop begins (so a reload before it counts);
/// zero means no limit.
pub async fn serve_until_stopped<F, E>(
    serve: F,
    lc: &Lifecycle,
    timeout: impl FnOnce() -> Duration,
) -> Result<(), E>
where
    F: Future<Output = Result<(), E>>,
{
    tokio::pin!(serve);
    tokio::select! {
        served = &mut serve => return served,
        () = lc.reached(Phase::Stopping) => {}
    }
    let timeout = timeout();
    let deadline = async {
        if timeout.is_zero() {
            std::future::pending().await
        } else {
            tokio::time::sleep(timeout).await
        }
    };
    tokio::select! {
        biased;
        served = &mut serve => served,
        () = lc.until_forced() => {
            tracing::warn!("stopping at once: closing the calls still open");
            Ok(())
        }
        () = deadline => {
            tracing::warn!(?timeout, "the drain timeout passed: closing the calls still open");
            Ok(())
        }
    }
}
