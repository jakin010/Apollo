//! Storage backends. `sqlite` is implemented; the others are future seams that
//! currently cause [`crate::open`] to return
//! [`StorageError::UnsupportedBackend`](crate::StorageError::UnsupportedBackend).

pub mod sqlite;
mod postgres;
mod surrealdb;

pub use sqlite::SqliteStorage;
