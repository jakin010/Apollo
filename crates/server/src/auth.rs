//! PASETO v4 (public / asymmetric) authentication for the `Inference` service.
//!
//! Tokens are minted out-of-band by the `apollo token` CLI, which signs a set of
//! claims with an Ed25519 **secret** key. The server is configured with only the
//! matching **public** key (`[auth].public_key`, PASERK `k4.public.…`) and
//! verifies the token on every `Inference` RPC via a tonic interceptor. Health and
//! reflection are deliberately left unauthenticated so load balancers and tooling
//! can probe them.
//!
//! Clients present the token in the `authorization` metadata, with or without a
//! `Bearer ` prefix.

use std::sync::Arc;

use pasetors::claims::ClaimsValidationRules;
use pasetors::keys::AsymmetricPublicKey;
use pasetors::token::UntrustedToken;
use pasetors::version4::V4;
use pasetors::Public;
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// Failure parsing the configured PASERK public key.
#[derive(Debug, thiserror::Error)]
#[error("invalid PASETO public key: {0}")]
pub struct KeyError(String);

/// Verifies a PASETO v4 public token on each request. Constructed with no key
/// (auth disabled), it passes every request through unchanged.
#[derive(Clone)]
pub struct AuthInterceptor {
    key: Option<Arc<AsymmetricPublicKey<V4>>>,
}

impl AuthInterceptor {
    /// Build from an optional PASERK public key (`k4.public.…`). `None` disables
    /// authentication (every request is allowed).
    pub fn new(public_key_paserk: Option<&str>) -> Result<Self, KeyError> {
        let key = match public_key_paserk {
            Some(s) => {
                let k =
                    AsymmetricPublicKey::<V4>::try_from(s).map_err(|e| KeyError(e.to_string()))?;
                Some(Arc::new(k))
            }
            None => None,
        };
        Ok(Self { key })
    }

    /// Whether authentication is enforced.
    pub fn is_enabled(&self) -> bool {
        self.key.is_some()
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        // Auth disabled -> allow.
        let Some(key) = self.key.as_deref() else {
            return Ok(request);
        };

        let raw = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization metadata"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization is not valid ASCII"))?;
        let token = raw.strip_prefix("Bearer ").unwrap_or(raw).trim();

        let untrusted = UntrustedToken::<Public, V4>::try_from(token)
            .map_err(|_| Status::unauthenticated("malformed token"))?;

        // Validate iat/nbf/exp; `allow_non_expiring` accepts long-lived API keys
        // (tokens that *do* carry an exp are still checked against it).
        let mut rules = ClaimsValidationRules::new();
        rules.allow_non_expiring();

        pasetors::public::verify(key, &untrusted, &rules, None, None)
            .map_err(|_| Status::unauthenticated("token verification failed"))?;

        Ok(request)
    }
}
