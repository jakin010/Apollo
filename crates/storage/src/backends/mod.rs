//! Storage backends. `sqlite` and `surreal` are implemented; `postgres` is a future seam that
//! currently causes [`crate::open`] to return
//! [`StorageError::UnsupportedBackend`](crate::StorageError::UnsupportedBackend).

pub mod sqlite;
pub mod surreal;
mod postgres;

pub use sqlite::SqliteStorage;
pub use surreal::SurrealStorage;
