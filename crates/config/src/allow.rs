//! `allow` lists: which TCP peers may connect to a monitoring listener.
//!
//! Entries are IPs or CIDRs, IPv4 or IPv6. Peers are matched by address
//! alone, after an IPv4-mapped IPv6 peer is turned back into IPv4.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use ipnet::IpNet;

/// The peers a listener admits. Built with [`Allowlist::parse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allowlist(Vec<IpNet>);

impl Allowlist {
    /// Parse `allow` entries. Each bad entry gives one message, starting with
    /// the entry and ending with a hint.
    pub fn parse(entries: &[String]) -> Result<Self, Vec<String>> {
        if entries.is_empty() {
            return Err(vec![
                "must list at least one IP or CIDR (leave it out to allow everyone)".into(),
            ]);
        }
        let mut nets = Vec::new();
        let mut errs = Vec::new();
        for entry in entries {
            match parse_entry(entry) {
                Ok(net) => nets.push(net),
                Err(hint) => errs.push(format!("`{entry}`: {hint}")),
            }
        }
        if errs.is_empty() {
            Ok(Self(nets))
        } else {
            Err(errs)
        }
    }

    /// An `allow` field parsed: `None` (the field left out) admits everyone.
    /// An invalid list is an error, never "everyone".
    pub fn parse_field(entries: Option<&[String]>) -> Result<Option<Self>, Vec<String>> {
        entries.map(Self::parse).transpose()
    }

    /// Whether `peer` is admitted. Never matches across address families.
    pub fn contains(&self, peer: IpAddr) -> bool {
        let peer = peer.to_canonical();
        self.0.iter().any(|net| net.contains(&peer))
    }

    /// Check the list against a socket bound to `local` (`only_v6`: an IPv6
    /// socket that refuses IPv4 peers). An error when no entry can ever
    /// match there; otherwise a warning for each entry that can't, and for
    /// each link-local entry.
    pub fn check_bind(&self, local: SocketAddr, only_v6: bool) -> Result<Vec<String>, String> {
        let reach = Reach::of(local.ip(), only_v6);
        let (live, dead): (Vec<&IpNet>, Vec<&IpNet>) = self
            .0
            .iter()
            .partition(|net| reach.nets().iter().any(|r| overlaps(r, net)));
        if live.is_empty() {
            return Err(format!(
                "no entry can match on {local}: {}",
                reach.describe()
            ));
        }
        let link_local = IpNet::V6("fe80::/10".parse().unwrap());
        let mut warnings: Vec<String> = dead
            .iter()
            .map(|net| {
                format!(
                    "`{}` can never match on {local}: {}",
                    Show(net),
                    reach.describe()
                )
            })
            .collect();
        warnings.extend(
            live.iter()
                .filter(|net| link_local.contains(**net))
                .map(|net| {
                    format!(
                        "`{}` is link-local: it matches on every interface (the zone is ignored)",
                        Show(net)
                    )
                }),
        );
        Ok(warnings)
    }

    /// The note for a socket bound to `local` that a local client, connecting
    /// over loopback, reaches but is refused at: `loopback refused` when every
    /// loopback address that reaches it is refused, or the one refused
    /// (`127.0.0.1 refused`) on a dual-stack socket that admits the other.
    pub fn loopback_note(&self, local: SocketAddr, only_v6: bool) -> Option<String> {
        let reachable: &[IpAddr] = match Reach::of(local.ip(), only_v6) {
            Reach::V4Loopback => &[IpAddr::V4(Ipv4Addr::LOCALHOST)],
            Reach::V6Loopback => &[IpAddr::V6(Ipv6Addr::LOCALHOST)],
            // A specific address is reached from itself, not from loopback.
            _ if !local.ip().is_unspecified() => &[],
            Reach::V4 => &[IpAddr::V4(Ipv4Addr::LOCALHOST)],
            Reach::V6 => &[IpAddr::V6(Ipv6Addr::LOCALHOST)],
            Reach::Both => &[
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                IpAddr::V6(Ipv6Addr::LOCALHOST),
            ],
        };
        let refused: Vec<&IpAddr> = reachable.iter().filter(|ip| !self.contains(**ip)).collect();
        match refused[..] {
            [] => None,
            [ip] if reachable.len() > 1 => Some(format!("{ip} refused")),
            _ => Some("loopback refused".into()),
        }
    }
}

/// The entries, comma-separated; a single address without its prefix.
impl fmt::Display for Allowlist {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, net) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{}", Show(net))?;
        }
        Ok(())
    }
}

/// A net as written in a list: `10.0.0.0/8`, or `10.0.0.1` for one address.
struct Show<'a>(&'a IpNet);

impl fmt::Display for Show<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let net = self.0;
        if net.prefix_len() == net.max_prefix_len() {
            write!(f, "{}", net.addr())
        } else {
            write!(f, "{net}")
        }
    }
}

/// The peers that can reach a bound socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reach {
    /// Bound to an IPv4 loopback address: 127.0.0.0/8.
    V4Loopback,
    /// Bound to `::1`: only `::1`.
    V6Loopback,
    V4,
    V6,
    /// Bound to `::` without `IPV6_V6ONLY`.
    Both,
}

impl Reach {
    fn of(ip: IpAddr, only_v6: bool) -> Self {
        match ip.to_canonical() {
            IpAddr::V4(v4) if v4.is_loopback() => Reach::V4Loopback,
            IpAddr::V4(_) => Reach::V4,
            IpAddr::V6(v6) if v6.is_loopback() => Reach::V6Loopback,
            IpAddr::V6(v6) if v6.is_unspecified() && !only_v6 => Reach::Both,
            IpAddr::V6(_) => Reach::V6,
        }
    }

    fn nets(self) -> Vec<IpNet> {
        let all_v4 = IpNet::V4(Default::default());
        let all_v6 = IpNet::V6(Default::default());
        match self {
            Reach::V4Loopback => vec![IpNet::V4("127.0.0.0/8".parse().unwrap())],
            Reach::V6Loopback => vec![IpNet::V6("::1/128".parse().unwrap())],
            Reach::V4 => vec![all_v4],
            Reach::V6 => vec![all_v6],
            Reach::Both => vec![all_v4, all_v6],
        }
    }

    fn describe(self) -> &'static str {
        match self {
            Reach::V4Loopback => "only IPv4 loopback peers (127.0.0.0/8) reach it",
            Reach::V6Loopback => "only ::1 reaches it",
            Reach::V4 => "only IPv4 peers reach it",
            Reach::V6 => "only IPv6 peers reach it",
            Reach::Both => "every peer can reach it",
        }
    }
}

/// Whether two nets share an address (CIDRs nest or are apart).
fn overlaps(a: &IpNet, b: &IpNet) -> bool {
    a.contains(b) || b.contains(a)
}

/// One entry, or the hint for what is wrong with it.
fn parse_entry(entry: &str) -> Result<IpNet, String> {
    if entry != entry.trim() {
        return Err("remove the spaces".into());
    }
    if entry.contains('%') {
        return Err(
            "zone ids are not allowed: write the address without `%…` (a link-local entry \
             matches on every interface)"
                .into(),
        );
    }
    if entry.starts_with('[') || entry.contains(']') {
        return Err(match entry.parse::<SocketAddr>() {
            Ok(sa) => format!("remove the brackets and the port: write `{}`", sa.ip()),
            Err(_) => format!(
                "remove the brackets: write `{}`",
                entry.replace(['[', ']'], "")
            ),
        });
    }
    let (addr, prefix) = match entry.split_once('/') {
        Some((addr, prefix)) => (addr, Some(prefix)),
        None => (entry, None),
    };
    let ip: IpAddr = addr.parse().map_err(|_| bad_address(entry, addr, prefix))?;
    let max = if ip.is_ipv4() { 32 } else { 128 };
    let prefix = match prefix {
        None => max,
        Some(p) => p
            .parse::<u8>()
            .ok()
            .filter(|n| p.bytes().all(|b| b.is_ascii_digit()) && *n <= max)
            .ok_or_else(|| format!("the prefix must be a number from 0 to {max}"))?,
    };
    let net = IpNet::new(ip, prefix).map_err(|e| e.to_string())?;
    if let IpAddr::V6(v6) = ip
        && let Some(v4) = v6.to_ipv4_mapped()
        && prefix >= 96
    {
        let v4 = IpNet::new(IpAddr::V4(v4), prefix - 96).map_err(|e| e.to_string())?;
        return Err(format!(
            "an IPv4-mapped address never matches (peers are matched as IPv4): write `{}`",
            Show(&v4.trunc())
        ));
    }
    if net != net.trunc() {
        return Err(format!("host bits are set: write `{}`", Show(&net.trunc())));
    }
    if ip.is_unspecified() && prefix == max {
        let all = if ip.is_ipv4() { "0.0.0.0/0" } else { "::/0" };
        return Err(format!(
            "this address matches no peer: write `{all}` to allow every {} peer",
            if ip.is_ipv4() { "IPv4" } else { "IPv6" }
        ));
    }
    let multicast = [
        IpNet::V4("224.0.0.0/4".parse().unwrap()),
        IpNet::V6("ff00::/8".parse().unwrap()),
    ];
    if multicast.iter().any(|m| m.contains(&net)) {
        return Err("multicast addresses never connect over TCP".into());
    }
    Ok(net)
}

/// The hint for an address std can't parse.
fn bad_address(entry: &str, addr: &str, prefix: Option<&str>) -> String {
    let octets: Vec<&str> = addr.split('.').collect();
    let dotted = octets.len() == 4
        && octets
            .iter()
            .all(|o| !o.is_empty() && o.bytes().all(|b| b.is_ascii_digit()));
    if dotted && octets.iter().any(|o| o.len() > 1 && o.starts_with('0')) {
        let fixed: Vec<&str> = octets
            .iter()
            .map(|o| match o.trim_start_matches('0') {
                "" => "0",
                rest => rest,
            })
            .collect();
        let fixed = fixed.join(".");
        if fixed.parse::<Ipv4Addr>().is_ok() {
            let fixed = match prefix {
                Some(p) => format!("{fixed}/{p}"),
                None => fixed,
            };
            return format!(
                "leading zeros are not allowed (some tools read them as octal): write `{fixed}`"
            );
        }
    }
    if let Ok(sa) = entry.parse::<SocketAddr>() {
        return format!("remove the port: write `{}`", sa.ip());
    }
    if addr.eq_ignore_ascii_case("localhost") {
        return "hostnames are not allowed: write `127.0.0.1` and `::1`".into();
    }
    if !addr.contains(':') && addr.bytes().any(|b| b.is_ascii_alphabetic()) {
        return "hostnames are not allowed: list the host's IP addresses".into();
    }
    "not an IP address or CIDR (e.g. `10.0.0.0/8`, `192.0.2.7`, `2001:db8::/32`)".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(entries: &[&str]) -> Allowlist {
        let entries: Vec<String> = entries.iter().map(|e| e.to_string()).collect();
        Allowlist::parse(&entries).unwrap()
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn at(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn a_mapped_peer_matches_an_ipv4_range() {
        let l = list(&["10.0.0.0/8"]);
        assert!(l.contains(ip("10.1.2.3")));
        assert!(l.contains(ip("::ffff:10.1.2.3")));
        assert!(!l.contains(ip("11.0.0.1")));
    }

    #[test]
    fn ipv6_loopback_is_not_ipv4_loopback() {
        assert!(!list(&["127.0.0.1"]).contains(ip("::1")));
        assert!(!list(&["::1"]).contains(ip("127.0.0.1")));
        assert!(list(&["::1"]).contains(ip("::1")));
    }

    #[test]
    fn each_everyone_form_matches_its_own_family() {
        let v4 = list(&["0.0.0.0/0"]);
        assert!(v4.contains(ip("192.0.2.7")));
        assert!(v4.contains(ip("::ffff:192.0.2.7")));
        assert!(!v4.contains(ip("2001:db8::1")));
        let v6 = list(&["::/0"]);
        assert!(v6.contains(ip("2001:db8::1")));
        // `::/0` alone refuses IPv4 peers, mapped or not.
        assert!(!v6.contains(ip("192.0.2.7")));
        assert!(!v6.contains(ip("::ffff:192.0.2.7")));
        let both = list(&["0.0.0.0/0", "::/0"]);
        assert!(both.contains(ip("192.0.2.7")) && both.contains(ip("2001:db8::1")));
    }

    #[test]
    fn a_list_that_cannot_match_the_bind_is_an_error() {
        let err = list(&["10.0.0.0/8"])
            .check_bind(at("127.0.0.1:9464"), false)
            .unwrap_err();
        assert!(err.contains("127.0.0.1:9464"), "{err}");
        assert!(err.contains("loopback"), "{err}");
        // IPv4 bind, IPv6 entries only.
        assert!(
            list(&["2001:db8::/32"])
                .check_bind(at("0.0.0.0:9464"), false)
                .is_err()
        );
        // `[::]` with IPV6_V6ONLY sees no IPv4 peer; without it, both.
        assert!(
            list(&["10.0.0.0/8"])
                .check_bind(at("[::]:9464"), true)
                .is_err()
        );
        assert_eq!(
            list(&["10.0.0.0/8", "2001:db8::/32"]).check_bind(at("[::]:9464"), false),
            Ok(vec![])
        );
    }

    #[test]
    fn a_dead_entry_gives_a_warning() {
        let warnings = list(&["10.0.0.0/8", "127.0.0.1"])
            .check_bind(at("127.0.0.1:9464"), false)
            .unwrap();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("`10.0.0.0/8` can never match"),
            "{warnings:?}"
        );
        // A 127/8 entry is live on a 127.0.0.1 bind (Linux clients may use any of it).
        assert_eq!(
            list(&["127.0.0.0/8"]).check_bind(at("127.0.0.1:9464"), false),
            Ok(vec![])
        );
    }

    #[test]
    fn a_link_local_entry_is_accepted_with_a_warning() {
        let l = list(&["fe80::1234:5678:9abc"]);
        assert!(l.contains(ip("fe80::1234:5678:9abc")));
        let warnings = l.check_bind(at("[::]:9465"), false).unwrap();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("zone is ignored"), "{warnings:?}");
    }

    #[test]
    fn loopback_refused_is_noted_where_a_local_client_reaches_the_socket() {
        let note = |entries: &[&str], local: &str, only_v6: bool| {
            list(entries).loopback_note(at(local), only_v6)
        };
        let refused = Some("loopback refused".to_string());
        assert_eq!(note(&["10.0.0.0/8"], "0.0.0.0:9464", false), refused);
        assert_eq!(note(&["10.0.0.0/8"], "10.0.0.5:9464", false), None);
        assert_eq!(
            note(&["10.0.0.0/8", "127.0.0.1"], "0.0.0.0:9464", false),
            None
        );
        assert_eq!(note(&["127.0.0.1"], "[::]:9464", true), refused);
        // Dual-stack `[::]`: each loopback address reaches it, and is noted on
        // its own when the other is admitted.
        let both = "[::]:9464";
        assert_eq!(note(&["10.0.0.0/8"], both, false), refused);
        assert_eq!(
            note(&["::1", "10.0.0.0/8"], both, false).as_deref(),
            Some("127.0.0.1 refused")
        );
        assert_eq!(
            note(&["127.0.0.1"], both, false).as_deref(),
            Some("::1 refused")
        );
        assert_eq!(note(&["127.0.0.1", "::1"], both, false), None);
        assert_eq!(list(&["10.0.0.0/8"]).to_string(), "10.0.0.0/8");
        assert_eq!(
            list(&["192.0.2.7", "::1/128"]).to_string(),
            "192.0.2.7, ::1"
        );
    }
}
