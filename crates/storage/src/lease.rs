//! Task leases: which instance runs a task, and until when.
//!
//! A task's claim is a random token, written when an instance creates the task
//! or takes it over. The runner renews it while it runs, and its writes only
//! apply while the claim is still its token (a [`Fence`]), so an instance whose
//! lease another took over can't overwrite the new runner's work. Lease times
//! come from the database's clock, never an instance's.

use std::time::Duration;

use apollo_domain::TaskState;

/// A claim to write with a task: [`Storage::create_task`] and
/// [`Storage::claim_task`](crate::Storage::claim_task).
///
/// [`Storage::create_task`]: crate::Storage::create_task
#[derive(Debug, Clone, Copy)]
pub struct Lease<'a> {
    /// The claim: fences the runner's writes.
    pub token: &'a str,
    /// The instance's node id.
    pub node: &'a str,
    /// The instance's process (one per start), for operators.
    pub process: &'a str,
    /// How long the claim lasts unless renewed.
    pub ttl: Duration,
}

/// The claim a runner's write requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fence<'a> {
    /// Only while the task's claim is this token.
    Claim(&'a str),
    /// Whatever the claim: a user's cancel, a planted row, a test.
    Unchecked,
}

impl<'a> Fence<'a> {
    /// The token, if the write is fenced.
    pub fn token(self) -> Option<&'a str> {
        match self {
            Fence::Claim(token) => Some(token),
            Fence::Unchecked => None,
        }
    }
}

/// Which tasks a claim may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimMode {
    /// Any unfinished task, whoever holds it: a database only this process
    /// uses (SQLite, at startup).
    Force,
    /// Only tasks nobody holds: stored before leases existed, or handed back
    /// by a graceful stop.
    Open,
    /// Those, and tasks whose lease expired (their holder stopped renewing it).
    Expired,
}

/// A task a claim may take (see [`Storage::claimable_tasks`]).
///
/// [`Storage::claimable_tasks`]: crate::Storage::claimable_tasks
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claimable {
    pub id: String,
    pub state: TaskState,
    pub created_at: i64,
    /// The node its input is local to, when that's another node whose lease
    /// ended more than the grace ago: the task can only be failed. (Never for
    /// a cancelled task: finishing it as cancelled reads no input.)
    pub abandoned_pin: Option<String>,
    /// Its input is local to this node: nobody else can run it.
    pub pinned_here: bool,
    /// Nobody has held it for more than the grace (or ever, since leases):
    /// if no instance that looked could run it, none will.
    pub stale: bool,
}

/// A claim this process holds.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Held {
    pub task_id: String,
    pub token: String,
}

/// What renewing one [`Held`] claim found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Renewal {
    /// Still held, and renewed. The task is now in this state.
    Held(TaskState),
    /// No longer held: taken over, or the task is gone.
    Lost,
    /// The renewal failed (a write conflict, say): try again.
    Unknown,
}
