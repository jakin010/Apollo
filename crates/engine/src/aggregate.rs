//! Result and lifecycle-state assembly.

use apollo_config::Aggregation;
use apollo_domain::{Classification, Frame, FrameScan, ItemState, Task, TaskState};

/// Whether an item has reached a terminal state.
pub(crate) fn item_terminal(state: ItemState) -> bool {
    matches!(
        state,
        ItemState::Completed | ItemState::Failed | ItemState::Cancelled
    )
}

/// Whether a task has reached a terminal (final) state.
pub(crate) fn task_terminal(state: TaskState) -> bool {
    matches!(
        state,
        TaskState::Completed | TaskState::Failed | TaskState::Cancelled
    )
}

/// The task state implied by its items: `Completed` once every item is terminal,
/// otherwise `Processing`. The single source of truth for the task-completion
/// invariant — the scheduler rolls the task up through this as each item finishes.
pub(crate) fn task_state_for(task: &Task) -> TaskState {
    if task.items.iter().all(|it| item_terminal(it.state)) {
        TaskState::Completed
    } else {
        TaskState::Processing
    }
}

/// Assemble a [`FrameScan`] from classified frames: order by index, then roll up
/// via the strategy aggregation (delegates to [`apollo_media::aggregate`]).
pub(crate) fn frame_scan(mut frames: Vec<Frame>, aggregation: Aggregation) -> FrameScan {
    frames.sort_by_key(|f| f.index);
    let per_frame: Vec<Classification> = frames.iter().map(|f| f.classification.clone()).collect();
    let aggregated = apollo_media::aggregate(&per_frame, aggregation);
    FrameScan { aggregated, frames }
}
