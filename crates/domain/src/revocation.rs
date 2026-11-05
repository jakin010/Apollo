//! Token revocations: a token id, or every token of a subject.

use std::fmt;

/// One revoked token, or every token of one subject: an entry of the
/// revocation file, or of a shared database's revocation list.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Revocation {
    /// A single token, by its id (`jti` claim).
    Id(String),
    /// Every token issued to a subject (`sub` claim), including tokens minted
    /// before token ids existed.
    Subject(String),
}

impl fmt::Display for Revocation {
    /// The line written to the revocation file.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.kind(), self.value())
    }
}

impl Revocation {
    /// Parse one line (`id <jti>` or `subject <name>`). Blank lines and `#`
    /// comments yield `Ok(None)`.
    pub fn parse_line(line: &str) -> Result<Option<Self>, String> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return Ok(None);
        }
        line.split_once(char::is_whitespace)
            .and_then(|(kind, value)| Self::from_parts(kind, value.trim()))
            .map(Some)
            .ok_or_else(|| format!("unrecognized revocation entry {line:?}"))
    }

    /// What it revokes by: `"id"` or `"subject"`.
    pub fn kind(&self) -> &'static str {
        match self {
            Revocation::Id(_) => "id",
            Revocation::Subject(_) => "subject",
        }
    }

    /// The token id or subject.
    pub fn value(&self) -> &str {
        match self {
            Revocation::Id(v) | Revocation::Subject(v) => v,
        }
    }

    /// The revocation of `kind` ([`Revocation::kind`]) and `value`; `None`
    /// for an unknown kind or an empty value.
    pub fn from_parts(kind: &str, value: &str) -> Option<Self> {
        if value.is_empty() {
            return None;
        }
        match kind {
            "id" => Some(Revocation::Id(value.into())),
            "subject" => Some(Revocation::Subject(value.into())),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revocation_lines_round_trip() {
        for r in [
            Revocation::Id("abc".into()),
            Revocation::Subject("ci".into()),
        ] {
            assert_eq!(Revocation::parse_line(&r.to_string()), Ok(Some(r)));
        }
        assert_eq!(Revocation::parse_line("  # note"), Ok(None));
        assert!(Revocation::parse_line("token abc").is_err());
        assert!(Revocation::parse_line("id   ").is_err());
    }

    #[test]
    fn parts_round_trip() {
        for r in [
            Revocation::Id("abc".into()),
            Revocation::Subject("ci".into()),
        ] {
            assert_eq!(Revocation::from_parts(r.kind(), r.value()), Some(r));
        }
        assert_eq!(Revocation::from_parts("token", "abc"), None);
        assert_eq!(Revocation::from_parts("id", ""), None);
    }
}
