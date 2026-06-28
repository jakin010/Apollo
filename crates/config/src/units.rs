//! Human-readable size parsing for byte-valued config fields ("4gb", "512mb").

/// Parse a byte size: a bare count (`1048576`) or a number with a binary-unit
/// suffix — `kb`/`k` = 1024, `mb`/`m` = 1024², `gb`/`g` = 1024³, `tb`/`t` = 1024⁴
/// (a trailing `b` alone means bytes). Case- and whitespace-insensitive; decimals
/// like `1.5gb` are accepted. `0` or an empty string means "no limit" → `None`.
pub fn parse_size(s: &str) -> Option<u64> {
    let t = s.trim().to_ascii_lowercase();
    if t.is_empty() || t == "0" {
        return None;
    }
    let (num, mult): (&str, u64) = if let Some(n) = strip(&t, "tb", "t") {
        (n, 1u64 << 40)
    } else if let Some(n) = strip(&t, "gb", "g") {
        (n, 1u64 << 30)
    } else if let Some(n) = strip(&t, "mb", "m") {
        (n, 1u64 << 20)
    } else if let Some(n) = strip(&t, "kb", "k") {
        (n, 1u64 << 10)
    } else if let Some(n) = t.strip_suffix('b') {
        (n, 1)
    } else {
        (t.as_str(), 1)
    };
    let num = num.trim();
    if let Ok(v) = num.parse::<u64>() {
        return Some(v.saturating_mul(mult));
    }
    if let Ok(f) = num.parse::<f64>() {
        if f.is_finite() && f >= 0.0 {
            return Some((f * mult as f64) as u64);
        }
    }
    None
}

fn strip<'a>(s: &'a str, long: &str, short: &str) -> Option<&'a str> {
    s.strip_suffix(long).or_else(|| s.strip_suffix(short))
}

#[cfg(test)]
mod tests {
    use super::parse_size;

    #[test]
    fn parses_units_and_bare_bytes() {
        assert_eq!(parse_size("4gb"), Some(4 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("512mb"), Some(512 * 1024 * 1024));
        assert_eq!(parse_size("2g"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("1024"), Some(1024));
        assert_eq!(parse_size("1.5gb"), Some(1024 * 1024 * 1024 + 512 * 1024 * 1024));
        assert_eq!(parse_size("0"), None);
        assert_eq!(parse_size(""), None);
        assert_eq!(parse_size("garbage"), None);
    }
}
