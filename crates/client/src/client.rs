//! The `Inference` client wrapper.

use std::sync::Arc;
use std::time::Duration;

use tonic::Request;
use tonic::transport::{Channel, Endpoint};

use apollo_proto::classify_chunk::Payload;
use apollo_proto::inference_client::InferenceClient;
use apollo_proto::{
    ClassifyChunk, ClassifyRequest, ClassifyStreamInit, GetTaskRequest, InputItem, Task,
};

use pasetors::claims::Claims;
use pasetors::keys::AsymmetricSecretKey;
use pasetors::version4::V4;

/// Bytes per `ClassifyStream` data frame. Well under the default 4 MiB gRPC
/// message ceiling.
const STREAM_CHUNK: usize = 64 * 1024;

/// Opening parameters for a [`Client::classify_stream`] / [`Client::classify_stream_bytes`]
/// call: which models (or a `pipeline`) to run, and whether the streamed bytes
/// are a video. Set **either** `models` or `pipeline`, mirroring `Classify`.
///
/// ```
/// # use apollo_client::StreamInit;
/// let init = StreamInit { models: vec!["nsfw".into()], video: true, ..Default::default() };
/// let pipe = StreamInit { pipeline: Some("moderation".into()), ..Default::default() };
/// ```
#[derive(Clone, Debug, Default)]
pub struct StreamInit {
    /// Model labels to run as a parallel set. Set this or `pipeline`.
    pub models: Vec<String>,
    /// A named pipeline to run instead of `models`.
    pub pipeline: Option<String>,
    /// `true` if the streamed bytes are a video, `false` for a single image.
    pub video: bool,
}

impl StreamInit {
    fn into_proto(self) -> ClassifyStreamInit {
        ClassifyStreamInit {
            models: self.models,
            video: self.video,
            pipeline: self.pipeline,
        }
    }
}

/// Errors talking to an apollo server.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc error: {0}")]
    Status(#[from] tonic::Status),
    #[error("auth error: {0}")]
    Auth(String),
}

/// Held secret key + claim settings used to sign a fresh PASETO v4 token for
/// each request.
struct Auth {
    secret: AsymmetricSecretKey<V4>,
    subject: String,
    ttl: Option<Duration>,
}

impl Auth {
    fn mint(&self) -> Result<String, ClientError> {
        let mut claims =
            Claims::new().map_err(|e| ClientError::Auth(format!("building claims: {e}")))?;
        claims
            .subject(&self.subject)
            .map_err(|e| ClientError::Auth(format!("setting subject: {e}")))?;
        match &self.ttl {
            Some(d) => claims
                .set_expires_in(d)
                .map_err(|e| ClientError::Auth(format!("setting expiry: {e}")))?,
            None => claims.non_expiring(),
        }
        pasetors::public::sign(&self.secret, &claims, None, None)
            .map_err(|e| ClientError::Auth(format!("signing token: {e}")))
    }
}

/// A connection to an apollo `Inference` server.
///
/// Cheap to clone (clones share one HTTP/2 channel) and safe to share across
/// tasks — the call methods take `&self`. Use [`Client::builder`] for full
/// control (including authentication), or the `connect*` shortcuts for the
/// unauthenticated case.
#[derive(Clone)]
pub struct Client {
    inner: InferenceClient<Channel>,
    auth: Option<Arc<Auth>>,
}

impl Client {
    /// Start building a client for `endpoint`. Add a secret key to authenticate.
    pub fn builder(endpoint: impl Into<String>) -> ClientBuilder {
        ClientBuilder::new(endpoint)
    }

    /// Connect eagerly with no authentication; errors immediately if the server
    /// is unreachable.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, ClientError> {
        let channel = Endpoint::from_shared(endpoint.into())?.connect().await?;
        Ok(Self {
            inner: InferenceClient::new(channel),
            auth: None,
        })
    }

    /// Build a client that connects on first use and reconnects automatically,
    /// with no authentication.
    pub fn connect_lazy(endpoint: impl Into<String>) -> Result<Self, ClientError> {
        let channel = Endpoint::from_shared(endpoint.into())?.connect_lazy();
        Ok(Self {
            inner: InferenceClient::new(channel),
            auth: None,
        })
    }

    /// Wrap a pre-built tonic [`Channel`] (custom TLS, interceptors, timeouts)
    /// with no automatic authentication.
    pub fn with_channel(channel: Channel) -> Self {
        Self {
            inner: InferenceClient::new(channel),
            auth: None,
        }
    }

    /// Wrap `msg` in a request, attaching a freshly minted `authorization` bearer
    /// token when the client was built with a secret key.
    fn request<T>(&self, msg: T) -> Result<Request<T>, ClientError> {
        let mut req = Request::new(msg);
        if let Some(auth) = &self.auth {
            let token = auth.mint()?;
            let value = format!("Bearer {token}")
                .parse()
                .map_err(|_| ClientError::Auth("minted token is not valid metadata".into()))?;
            req.metadata_mut().insert("authorization", value);
        }
        Ok(req)
    }

    /// Submit a single input. Returns the new task id. Build `item` with the
    /// [`crate::item`] helpers.
    pub async fn classify(&self, item: InputItem) -> Result<String, ClientError> {
        let req = self.request(ClassifyRequest { item: Some(item) })?;
        let resp = self.inner.clone().classify(req).await?;
        Ok(resp.into_inner().task_id)
    }

    /// Stream the raw bytes of a single input and create a task for it, feeding a
    /// stream of byte chunks — keep yielding buffers until the stream ends, then
    /// the task is submitted. Use this to stream a large file (or a live source)
    /// without buffering it all in memory: wrap a reader with
    /// `tokio_util::io::ReaderStream`, drive a channel with
    /// `tokio_stream::wrappers::ReceiverStream`, or pass `futures::stream::iter`.
    /// Each yielded buffer is fragmented into ≤64 KiB content frames. Returns the
    /// task id.
    ///
    /// The server stages the bytes to disk as they arrive and enforces the upload
    /// cap incrementally, so nothing has to be held in memory end to end.
    pub async fn classify_stream(
        &self,
        init: StreamInit,
        chunks: impl futures::Stream<Item = Vec<u8>> + Send + 'static,
    ) -> Result<String, ClientError> {
        use futures::StreamExt;
        let init_frame = ClassifyChunk {
            payload: Some(Payload::Init(init.into_proto())),
        };
        // The init frame first, then every buffer fragmented into wire-sized
        // content frames, in order.
        let data_frames = chunks.flat_map(|buf| {
            let frames: Vec<ClassifyChunk> = buf
                .chunks(STREAM_CHUNK)
                .map(|c| ClassifyChunk {
                    payload: Some(Payload::Data(c.to_vec())),
                })
                .collect();
            futures::stream::iter(frames)
        });
        let stream = futures::stream::once(async move { init_frame }).chain(data_frames);
        let req = self.request(stream)?;
        let resp = self.inner.clone().classify_stream(req).await?;
        Ok(resp.into_inner().task_id)
    }

    /// Convenience over [`classify_stream`](Self::classify_stream) for content
    /// already fully in memory: streams a single buffer.
    pub async fn classify_stream_bytes(
        &self,
        init: StreamInit,
        data: Vec<u8>,
    ) -> Result<String, ClientError> {
        self.classify_stream(init, futures::stream::once(async move { data }))
            .await
    }

    /// Poll a task's state and per-item / per-model results.
    pub async fn get_task(&self, task_id: impl Into<String>) -> Result<Task, ClientError> {
        let req = self.request(GetTaskRequest {
            task_id: task_id.into(),
        })?;
        let resp = self.inner.clone().get_task(req).await?;
        Ok(resp.into_inner())
    }
}

/// Builder for [`Client`], including optional automatic token minting.
///
/// ```no_run
/// # async fn f() -> Result<(), apollo_client::ClientError> {
/// use std::time::Duration;
/// let client = apollo_client::Client::builder("http://127.0.0.1:8080")
///     .secret_key("k4.secret.…")
///     .subject("ingest-worker")
///     .token_ttl(Duration::from_secs(300))
///     .build()
///     .await?;
/// # Ok(()) }
/// ```
pub struct ClientBuilder {
    endpoint: String,
    lazy: bool,
    secret_key: Option<String>,
    subject: String,
    ttl: Option<Duration>,
}

impl ClientBuilder {
    fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            lazy: false,
            secret_key: None,
            subject: "apollo-client".to_string(),
            ttl: None,
        }
    }

    /// Connect on first use (and reconnect automatically) instead of eagerly.
    pub fn lazy(mut self, lazy: bool) -> Self {
        self.lazy = lazy;
        self
    }

    /// Authenticate by minting a PASETO token from this PASERK secret key
    /// (`k4.secret.…`) on every request. Without it, requests are sent
    /// unauthenticated.
    pub fn secret_key(mut self, paserk: impl Into<String>) -> Self {
        self.secret_key = Some(paserk.into());
        self
    }

    /// Subject (`sub`) claim placed in minted tokens. Defaults to `apollo-client`.
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = subject.into();
        self
    }

    /// Expiry for minted tokens. Omit for non-expiring tokens; either way a fresh
    /// token is signed per request, so short TTLs are cheap and safe.
    pub fn token_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Resolve the endpoint, parse the secret key (if any), and connect.
    pub async fn build(self) -> Result<Client, ClientError> {
        let endpoint = Endpoint::from_shared(self.endpoint)?;
        let channel = if self.lazy {
            endpoint.connect_lazy()
        } else {
            endpoint.connect().await?
        };
        let auth = match self.secret_key {
            Some(paserk) => {
                let secret = AsymmetricSecretKey::<V4>::try_from(paserk.trim())
                    .map_err(|e| ClientError::Auth(format!("parsing secret key: {e}")))?;
                Some(Arc::new(Auth {
                    secret,
                    subject: self.subject,
                    ttl: self.ttl,
                }))
            }
            None => None,
        };
        Ok(Client {
            inner: InferenceClient::new(channel),
            auth,
        })
    }
}
