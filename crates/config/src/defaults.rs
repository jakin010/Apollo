//! Default values for optional config fields.
//!
//! Referenced from `schema` via `#[serde(default = "crate::defaults::...")]` and
//! mirrored in the `Default` impls there, so a missing field and a missing
//! section resolve to the same value.

use crate::schema::Aggregation;

pub(crate) fn endpoint() -> String {
    "0.0.0.0".to_string()
}
pub(crate) fn port() -> u16 {
    8080
}
pub(crate) fn global_max_concurrent() -> u32 {
    20
}
pub(crate) fn idle_timeout() -> u32 {
    300
}
pub(crate) fn log_level() -> String {
    "info".to_string()
}
pub(crate) fn revision() -> String {
    "main".to_string()
}
pub(crate) fn enabled() -> bool {
    true
}
pub(crate) fn model_max_concurrent() -> u32 {
    8
}
pub(crate) fn timeout() -> u32 {
    30
}
pub(crate) fn early_exit_threshold() -> f32 {
    0.85
}
pub(crate) fn aggregation() -> Aggregation {
    Aggregation::Mean
}
pub(crate) fn sqlite_path() -> String {
    "apollo.db".to_string()
}
pub(crate) fn wal() -> bool {
    true
}
pub(crate) fn busy_timeout() -> u32 {
    5000
}
pub(crate) fn sqlite_max_connections() -> u32 {
    5
}
pub(crate) fn pg_port() -> u16 {
    5432
}
pub(crate) fn pg_max_connections() -> u32 {
    10
}
