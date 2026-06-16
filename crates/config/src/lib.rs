//! `apollo-config` — TOML configuration: schema, defaults, load, validate, edit.
//!
//! - [`schema`]   — serde structs mirroring the TOML
//! - [`defaults`] — default values for optional fields
//! - [`load`]     — discovery + parsing (resolution order) + run-only overrides
//! - [`validate`] — invariants (reference integrity, sampling params, ...)
//! - [`edit`]     — format-preserving `get` / `set` / `remove` + default creation
//! - [`error`]    — [`ConfigError`]

pub mod defaults;
pub mod edit;
pub mod error;
pub mod load;
pub mod schema;
pub mod validate;

#[cfg(test)]
mod tests;

pub use error::ConfigError;
pub use load::Overrides;
pub use schema::{
    Aggregation, AppConfig, Backend, Config, DatabaseConfig, EarlyExit, ModelConfig,
    PostgresConfig, SamplingKind, SamplingStep, SqliteConfig, StrategyConfig,
    SurrealdbConfig, WebhookConfig,
};
