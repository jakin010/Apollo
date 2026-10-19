//! The `Inference` client wrapper.

use std::sync::Arc;
use std::time::Duration;

use tonic::Request;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use std::collections::BTreeMap;

use apollo_proto::classify_chunk::Payload;
use apollo_proto::classify_stream_init::Run;
use apollo_proto::inference_client::InferenceClient;
use apollo_proto::{
    CancelTaskRequest, ClassifyChunk, ClassifyRequest, ClassifyStreamInit, GetTaskRequest,
    ListModelsRequest, ListModelsResponse, ListTasksRequest, ListTasksResponse, MediaKind,
    ModelList, ResultView, Task, WatchTaskRequest,
};

use pasetors::claims::Claims;
use pasetors::keys::AsymmetricSecretKey;
use pasetors::version4::V4;

/// Bytes per `ClassifyStream` data frame. Well under the default 4 MiB gRPC
/// message ceiling.
const STREAM_CHUNK: usize = 64 * 1024;

/// Default lifetime of a minted auth token. A fresh token is signed per request,
/// so a short lifetime costs nothing and limits the damage of a leaked token.
const DEFAULT_TOKEN_TTL: Duration = Duration::from_secs(5 * 60);

/// Parse `endpoint`, enabling TLS (system + webpki trust roots) for `https://`.
fn endpoint(endpoint: impl Into<String>) -> Result<Endpoint, ClientError> {
    endpoint_with_tls(endpoint, ClientTlsConfig::new())
}

/// Parse `endpoint`; for `https://`, apply `tls` on top of the system + webpki
/// trust roots.
fn endpoint_with_tls(
    endpoint: impl Into<String>,
    tls: ClientTlsConfig,
) -> Result<Endpoint, ClientError> {
    let ep = Endpoint::from_shared(endpoint.into())?;
    if ep.uri().scheme_str() == Some("https") {
        return Ok(ep.tls_config(tls.with_enabled_roots())?);
    }
    Ok(ep)
}

/// Opening parameters for a [`Client::classify_stream`] /
/// [`Client::classify_stream_bytes`] call: what the bytes are, what to run on
/// them, and the same optional fields as `Classify`.
///
/// ```
/// # use apollo_client::{MediaKind, StreamInit};
/// let init = StreamInit::models(MediaKind::Video, ["nsfw"]);
/// let mut pipe = StreamInit::pipeline(MediaKind::Image, "moderation");
/// pipe.wait_seconds = 30;
/// ```
#[derive(Clone, Debug)]
pub struct StreamInit {
    pub kind: MediaKind,
    /// Models to run concurrently. Set this or `pipeline`.
    pub models: Vec<String>,
    /// A named pipeline to run instead of `models`.
    pub pipeline: Option<String>,
    /// Echoed back on the task and in webhooks.
    pub metadata: BTreeMap<String, String>,
    /// Idempotency key: resubmitting with it returns the existing task.
    pub request_id: Option<String>,
    /// Wait up to this many seconds (at most 300) for the task to finish.
    pub wait_seconds: u32,
    pub view: ResultView,
}

impl StreamInit {
    /// Run `models` on streamed content of `kind`.
    pub fn models(kind: MediaKind, models: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            kind,
            models: models.into_iter().map(Into::into).collect(),
            pipeline: None,
            metadata: BTreeMap::new(),
            request_id: None,
            wait_seconds: 0,
            view: ResultView::Summary,
        }
    }

    /// Run the named pipeline on streamed content of `kind`.
    pub fn pipeline(kind: MediaKind, pipeline: impl Into<String>) -> Self {
        Self {
            pipeline: Some(pipeline.into()),
            ..Self::models(kind, Vec::<String>::new())
        }
    }

    fn into_proto(self) -> ClassifyStreamInit {
        let run = match self.pipeline {
            Some(p) => Run::Pipeline(p),
            None => Run::Models(ModelList { names: self.models }),
        };
        ClassifyStreamInit {
            kind: self.kind as i32,
            run: Some(run),
            metadata: self.metadata,
            request_id: self.request_id.unwrap_or_default(),
            wait_seconds: self.wait_seconds,
            view: self.view as i32,
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
    #[error("invalid client configuration: {0}")]
    Config(String),
}

/// How requests are authenticated.
enum Auth {
    /// A pre-minted token (e.g. from `apollo token`), sent as-is.
    Token(String),
    /// A secret key + claim settings used to sign a fresh PASETO v4 token for
    /// each request.
    Mint {
        secret: AsymmetricSecretKey<V4>,
        subject: String,
        ttl: Option<Duration>,
    },
}

impl Auth {
    /// The bearer token for the next request.
    fn token(&self) -> Result<String, ClientError> {
        let (secret, subject, ttl) = match self {
            Auth::Token(token) => return Ok(token.clone()),
            Auth::Mint {
                secret,
                subject,
                ttl,
            } => (secret, subject, ttl),
        };
        let mut claims =
            Claims::new().map_err(|e| ClientError::Auth(format!("building claims: {e}")))?;
        claims
            .subject(subject)
            .map_err(|e| ClientError::Auth(format!("setting subject: {e}")))?;
        match ttl {
            Some(d) => claims
                .set_expires_in(d)
                .map_err(|e| ClientError::Auth(format!("setting expiry: {e}")))?,
            None => claims.non_expiring(),
        }
        pasetors::public::sign(secret, &claims, None, None)
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
    /// is unreachable. `https://` endpoints use TLS.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, ClientError> {
        let channel = self::endpoint(endpoint)?.connect().await?;
        Ok(Self {
            inner: InferenceClient::new(channel),
            auth: None,
        })
    }

    /// Build a client that connects on first use and reconnects automatically,
    /// with no authentication. `https://` endpoints use TLS.
    pub fn connect_lazy(endpoint: impl Into<String>) -> Result<Self, ClientError> {
        let channel = self::endpoint(endpoint)?.connect_lazy();
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
            let token = auth.token()?;
            let value = format!("Bearer {token}")
                .parse()
                .map_err(|_| ClientError::Auth("minted token is not valid metadata".into()))?;
            req.metadata_mut().insert("authorization", value);
        }
        Ok(req)
    }

    /// Submit a single input. Returns the task: queued, or finished if the
    /// request's `wait_seconds` allowed for it. Build `request` with the
    /// [`crate::item`] helpers.
    pub async fn classify(&self, request: ClassifyRequest) -> Result<Task, ClientError> {
        let req = self.request(request)?;
        let resp = self.inner.clone().classify(req).await?;
        Ok(resp.into_inner())
    }

    /// Stream the raw bytes of a single input and create a task for it, feeding a
    /// stream of byte chunks — keep yielding buffers until the stream ends, then
    /// the task is submitted. Use this to stream a large file (or a live source)
    /// without buffering it all in memory: wrap a reader with
    /// `tokio_util::io::ReaderStream`, drive a channel with
    /// `tokio_stream::wrappers::ReceiverStream`, or pass `futures::stream::iter`.
    /// Each yielded buffer is fragmented into ≤64 KiB content frames. Returns the
    /// task, as [`classify`](Self::classify) does.
    ///
    /// The server stages the bytes to disk as they arrive and enforces the upload
    /// cap incrementally, so nothing has to be held in memory end to end.
    pub async fn classify_stream(
        &self,
        init: StreamInit,
        chunks: impl futures::Stream<Item = Vec<u8>> + Send + 'static,
    ) -> Result<Task, ClientError> {
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
        Ok(resp.into_inner())
    }

    /// Convenience over [`classify_stream`](Self::classify_stream) for content
    /// already fully in memory: streams a single buffer.
    pub async fn classify_stream_bytes(
        &self,
        init: StreamInit,
        data: Vec<u8>,
    ) -> Result<Task, ClientError> {
        self.classify_stream(init, futures::stream::once(async move { data }))
            .await
    }

    /// A task's state and per-model results (summary view: scans without
    /// their per-sample results).
    pub async fn get_task(&self, task_id: impl Into<String>) -> Result<Task, ClientError> {
        self.get_task_view(task_id, ResultView::Summary).await
    }

    /// [`get_task`](Self::get_task) with the given result detail.
    pub async fn get_task_view(
        &self,
        task_id: impl Into<String>,
        view: ResultView,
    ) -> Result<Task, ClientError> {
        let req = self.request(GetTaskRequest {
            task_id: task_id.into(),
            view: view as i32,
        })?;
        let resp = self.inner.clone().get_task(req).await?;
        Ok(resp.into_inner())
    }

    /// Follow a task: the task now, then again each time it changes; the
    /// stream ends once the task is completed, failed or cancelled.
    pub async fn watch_task(
        &self,
        task_id: impl Into<String>,
        view: ResultView,
    ) -> Result<futures::stream::BoxStream<'static, Result<Task, ClientError>>, ClientError> {
        use futures::StreamExt;
        let req = self.request(WatchTaskRequest {
            task_id: task_id.into(),
            view: view as i32,
        })?;
        let stream = self.inner.clone().watch_task(req).await?.into_inner();
        Ok(stream.map(|r| r.map_err(ClientError::from)).boxed())
    }

    /// Your tasks, newest first, a page at a time (pass the response's
    /// `next_page_token` as the next request's `page_token`).
    pub async fn list_tasks(
        &self,
        request: ListTasksRequest,
    ) -> Result<ListTasksResponse, ClientError> {
        let req = self.request(request)?;
        Ok(self.inner.clone().list_tasks(req).await?.into_inner())
    }

    /// Request cooperative cancellation of a task. Returns the task as it stands
    /// after the request: `CANCELLED`, unless it had already finished.
    pub async fn cancel_task(&self, task_id: impl Into<String>) -> Result<Task, ClientError> {
        let req = self.request(CancelTaskRequest {
            task_id: task_id.into(),
        })?;
        let resp = self.inner.clone().cancel_task(req).await?;
        Ok(resp.into_inner())
    }

    /// The server's models (with their labels) and pipelines.
    pub async fn list_models(&self) -> Result<ListModelsResponse, ClientError> {
        let req = self.request(ListModelsRequest {})?;
        Ok(self.inner.clone().list_models(req).await?.into_inner())
    }
}

/// Builder for [`Client`], including authentication. The server requires a
/// token by default; give the client either a token minted by `apollo token`
/// (preferred — the client never holds the signing key) or the secret key, in
/// which case a short-lived token is minted per request.
///
/// ```no_run
/// # async fn f() -> Result<(), apollo_client::ClientError> {
/// let client = apollo_client::Client::builder("https://apollo.internal:8080")
///     .token("v4.public.…") // from `apollo token --subject ingest-worker`
///     .build()
///     .await?;
/// # Ok(()) }
/// ```
pub struct ClientBuilder {
    endpoint: String,
    lazy: bool,
    token: Option<String>,
    secret_key: Option<String>,
    subject: String,
    ttl: Option<Duration>,
    ca_pem: Option<Vec<u8>>,
    identity: Option<(Vec<u8>, Vec<u8>)>,
    tls_domain: Option<String>,
}

impl ClientBuilder {
    fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            lazy: false,
            token: None,
            secret_key: None,
            subject: "apollo-client".to_string(),
            ttl: Some(DEFAULT_TOKEN_TTL),
            ca_pem: None,
            identity: None,
            tls_domain: None,
        }
    }

    /// Also trust this PEM CA certificate for an `https://` endpoint (e.g. a
    /// private CA or a self-signed server certificate). The system and webpki
    /// roots stay trusted.
    pub fn ca_certificate(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem = Some(pem.into());
        self
    }

    /// Present this PEM certificate + private key to the server (mutual TLS, for
    /// servers configured with `[tls].client_ca_file`).
    pub fn client_identity(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.identity = Some((cert_pem.into(), key_pem.into()));
        self
    }

    /// Verify the server certificate against this name instead of the
    /// endpoint's host (e.g. connecting by IP to a certificate issued for a
    /// hostname).
    pub fn tls_domain_name(mut self, name: impl Into<String>) -> Self {
        self.tls_domain = Some(name.into());
        self
    }

    /// Connect on first use (and reconnect automatically) instead of eagerly.
    pub fn lazy(mut self, lazy: bool) -> Self {
        self.lazy = lazy;
        self
    }

    /// Authenticate with a pre-minted token (from `apollo token`), sent on every
    /// request. Takes precedence over [`secret_key`](Self::secret_key).
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Authenticate by minting a PASETO token from this PASERK secret key
    /// (`k4.secret.…`) on every request. Prefer [`token`](Self::token) where
    /// possible: whoever holds the secret key can mint tokens for any subject.
    /// Without a token or key, requests are sent unauthenticated.
    pub fn secret_key(mut self, paserk: impl Into<String>) -> Self {
        self.secret_key = Some(paserk.into());
        self
    }

    /// Subject (`sub`) claim placed in minted tokens. Defaults to `apollo-client`.
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = subject.into();
        self
    }

    /// Expiry for minted tokens (default 5 minutes). A fresh token is signed per
    /// request, so short TTLs are cheap and safe.
    pub fn token_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Mint tokens without an expiry. Not recommended: a leaked non-expiring
    /// token stays valid until the server's keypair is rotated.
    pub fn non_expiring_tokens(mut self) -> Self {
        self.ttl = None;
        self
    }

    /// Resolve the endpoint (TLS for `https://`), set up authentication, and
    /// connect.
    pub async fn build(self) -> Result<Client, ClientError> {
        let custom_tls =
            self.ca_pem.is_some() || self.identity.is_some() || self.tls_domain.is_some();
        if custom_tls && !self.endpoint.starts_with("https://") {
            return Err(ClientError::Config(
                "TLS options (CA certificate, client identity, domain name) need an https:// endpoint"
                    .into(),
            ));
        }
        let mut tls = ClientTlsConfig::new();
        if let Some(ca) = &self.ca_pem {
            tls = tls.ca_certificate(Certificate::from_pem(ca));
        }
        if let Some((cert, key)) = &self.identity {
            tls = tls.identity(Identity::from_pem(cert, key));
        }
        if let Some(domain) = self.tls_domain {
            tls = tls.domain_name(domain);
        }
        let endpoint = endpoint_with_tls(self.endpoint, tls)?;
        let channel = if self.lazy {
            endpoint.connect_lazy()
        } else {
            endpoint.connect().await?
        };
        let auth = match (self.token, self.secret_key) {
            (Some(token), _) => Some(Arc::new(Auth::Token(token.trim().to_string()))),
            (None, Some(paserk)) => {
                let secret = AsymmetricSecretKey::<V4>::try_from(paserk.trim())
                    .map_err(|e| ClientError::Auth(format!("parsing secret key: {e}")))?;
                Some(Arc::new(Auth::Mint {
                    secret,
                    subject: self.subject,
                    ttl: self.ttl,
                }))
            }
            (None, None) => None,
        };
        Ok(Client {
            inner: InferenceClient::new(channel),
            auth,
        })
    }
}
