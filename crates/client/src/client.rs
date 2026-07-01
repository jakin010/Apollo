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

    /// Stream the raw bytes of a single image or video and create a task for it.
    /// `video` selects the processing path (false = image). `data` is sent as an
    /// opening init frame followed by 64 KiB content frames. Returns the task id.
    pub async fn classify_stream(
        &self,
        models: Vec<String>,
        video: bool,
        data: Vec<u8>,
    ) -> Result<String, ClientError> {
        let mut msgs = Vec::with_capacity(data.len() / STREAM_CHUNK + 2);
        msgs.push(ClassifyChunk {
            payload: Some(Payload::Init(ClassifyStreamInit { models, video })),
        });
        for chunk in data.chunks(STREAM_CHUNK) {
            msgs.push(ClassifyChunk {
                payload: Some(Payload::Data(chunk.to_vec())),
            });
        }
        let req = self.request(futures::stream::iter(msgs))?;
        let resp = self.inner.clone().classify_stream(req).await?;
        Ok(resp.into_inner().task_id)
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
