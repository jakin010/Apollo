//! Parsing of the `[database].retention` string into a duration.

use std::time::Duration;

/// Parse a retention string like `"30d"`, `"12h"`, `"90m"`, `"45s"`, or `"2w"`.
/// Returns `None` if empty or malformed. A bare number (no unit) is rejected.
pub fn parse(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let split = s.find(|c: char| !c.is_ascii_digit())?;
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().ok()?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3_600,
        "d" => n * 86_400,
        "w" => n * 604_800,
        _ => return None,
    };
    Some(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::parse;
    use std::time::Duration;

    #[test]
    fn parses_supported_units() {
        assert_eq!(parse("45s"), Some(Duration::from_secs(45)));
        assert_eq!(parse("90m"), Some(Duration::from_secs(90 * 60)));
        assert_eq!(parse("12h"), Some(Duration::from_secs(12 * 3_600)));
        assert_eq!(parse("30d"), Some(Duration::from_secs(30 * 86_400)));
        assert_eq!(parse("2w"), Some(Duration::from_secs(2 * 604_800)));
    }

    #[test]
    fn rejects_malformed() {
        assert_eq!(parse(""), None);
        assert_eq!(parse("30"), None); // no unit
        assert_eq!(parse("abc"), None);
        assert_eq!(parse("10y"), None); // unknown unit
    }
}
