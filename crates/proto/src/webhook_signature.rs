//! Webhook delivery signing, scheme `v1`.
//!
//! When `[webhook].secret` is set, every delivery carries two metadata headers:
//!
//! ```text
//! x-apollo-webhook-timestamp: <unix seconds when the delivery was sent>
//! x-apollo-webhook-signature: v1=<lowercase hex HMAC-SHA256(secret, "<timestamp>." || payload)>
//! ```
//!
//! `payload` is the protobuf-encoded `Task` exactly as sent: the gRPC message
//! bytes, without the 5-byte gRPC frame header. Binding the signature to the
//! payload means a captured delivery cannot be altered, and binding it to the
//! timestamp lets a receiver reject old deliveries replayed later.
//!
//! Receivers should verify against the raw request bytes. Where a framework only
//! exposes the decoded message, re-encode it with *deterministic* serialization
//! (fields in number order, map entries sorted by key — e.g. Go's
//! `proto.MarshalOptions{Deterministic: true}`): Apollo encodes that way, so the
//! bytes match as long as both sides use the same schema version.

use std::time::Duration;

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

/// Metadata header carrying the `v1=<hex>` signature.
pub const SIGNATURE_HEADER: &str = "x-apollo-webhook-signature";
/// Metadata header carrying the unix-seconds timestamp that was signed.
pub const TIMESTAMP_HEADER: &str = "x-apollo-webhook-timestamp";
/// Default maximum age (and future skew) of a delivery's timestamp.
pub const DEFAULT_TOLERANCE: Duration = Duration::from_secs(5 * 60);

const SCHEME: &str = "v1=";

/// Why a delivery's signature was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SignatureError {
    #[error("missing webhook signature or timestamp")]
    Missing,
    #[error("malformed webhook signature or timestamp")]
    Malformed,
    #[error("webhook timestamp is outside the allowed window ({age_secs}s off)")]
    Stale { age_secs: i64 },
    #[error("invalid webhook signature")]
    Mismatch,
}

fn mac(secret: &[u8], timestamp: i64, payload: &[u8]) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts a key of any length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(payload);
    mac
}

/// The `x-apollo-webhook-signature` value for `payload` sent at `timestamp`.
pub fn sign(secret: &[u8], timestamp: i64, payload: &[u8]) -> String {
    use std::fmt::Write;
    let bytes = mac(secret, timestamp, payload).finalize().into_bytes();
    let mut out = String::with_capacity(SCHEME.len() + bytes.len() * 2);
    out.push_str(SCHEME);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Verify a delivery: the header values as received, the raw message bytes, the
/// receiver's current unix time, and the accepted clock window. The comparison is
/// constant-time.
pub fn verify(
    secret: &[u8],
    signature: Option<&str>,
    timestamp: Option<&str>,
    payload: &[u8],
    now: i64,
    tolerance: Duration,
) -> Result<(), SignatureError> {
    let (Some(signature), Some(timestamp)) = (signature, timestamp) else {
        return Err(SignatureError::Missing);
    };
    let ts: i64 = timestamp
        .trim()
        .parse()
        .map_err(|_| SignatureError::Malformed)?;
    let age = now.saturating_sub(ts);
    if age.unsigned_abs() > tolerance.as_secs() {
        return Err(SignatureError::Stale { age_secs: age });
    }
    let hex = signature
        .trim()
        .strip_prefix(SCHEME)
        .ok_or(SignatureError::Malformed)?;
    let provided = hex_decode(hex).ok_or(SignatureError::Malformed)?;
    mac(secret, ts, payload)
        .verify_slice(&provided)
        .map_err(|_| SignatureError::Mismatch)
}

/// Decode a hex string (either case) to bytes, or `None` if malformed.
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    fn nibble(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
    s.as_bytes()
        .chunks(2)
        .map(|pair| Some((nibble(pair[0])? << 4) | nibble(pair[1])?))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &[u8] = b"shared-secret";
    const NOW: i64 = 1_800_000_000;

    fn check(sig: &str, ts: &str, payload: &[u8]) -> Result<(), SignatureError> {
        verify(KEY, Some(sig), Some(ts), payload, NOW, DEFAULT_TOLERANCE)
    }

    #[test]
    fn round_trips() {
        let sig = sign(KEY, NOW, b"payload");
        assert!(sig.starts_with("v1=") && sig.len() == 3 + 64);
        assert_eq!(check(&sig, &NOW.to_string(), b"payload"), Ok(()));
        // Upper-case hex is accepted too.
        let upper = format!("v1={}", sig[3..].to_uppercase());
        assert_eq!(check(&upper, &NOW.to_string(), b"payload"), Ok(()));
    }

    #[test]
    fn rejects_tampering() {
        let sig = sign(KEY, NOW, b"payload");
        assert_eq!(
            check(&sig, &NOW.to_string(), b"payloaX"),
            Err(SignatureError::Mismatch)
        );
        // Moving the timestamp invalidates the signature even inside the window.
        assert_eq!(
            check(&sig, &(NOW - 1).to_string(), b"payload"),
            Err(SignatureError::Mismatch)
        );
        let other = verify(
            b"other-secret",
            Some(&sig),
            Some(&NOW.to_string()),
            b"payload",
            NOW,
            DEFAULT_TOLERANCE,
        );
        assert_eq!(other, Err(SignatureError::Mismatch));
    }

    #[test]
    fn rejects_replays_outside_the_window() {
        let old = NOW - 301;
        let sig = sign(KEY, old, b"p");
        assert_eq!(
            check(&sig, &old.to_string(), b"p"),
            Err(SignatureError::Stale { age_secs: 301 })
        );
        let future = NOW + 301;
        let sig = sign(KEY, future, b"p");
        assert!(matches!(
            check(&sig, &future.to_string(), b"p"),
            Err(SignatureError::Stale { .. })
        ));
        let edge = NOW - 300;
        assert_eq!(
            check(&sign(KEY, edge, b"p"), &edge.to_string(), b"p"),
            Ok(())
        );
    }

    #[test]
    fn rejects_missing_and_malformed() {
        let ts = NOW.to_string();
        assert_eq!(
            verify(KEY, None, Some(&ts), b"p", NOW, DEFAULT_TOLERANCE),
            Err(SignatureError::Missing)
        );
        assert_eq!(
            verify(KEY, Some("v1=00"), None, b"p", NOW, DEFAULT_TOLERANCE),
            Err(SignatureError::Missing)
        );
        let bare = sign(KEY, NOW, b"p")[3..].to_string(); // no "v1=" prefix
        assert_eq!(check(&bare, &ts, b"p"), Err(SignatureError::Malformed));
        assert_eq!(check("v1=zz", &ts, b"p"), Err(SignatureError::Malformed));
        assert_eq!(check("v1=00", "soon", b"p"), Err(SignatureError::Malformed));
    }
}
