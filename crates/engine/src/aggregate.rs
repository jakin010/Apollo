//! Result and lifecycle-state assembly.

use apollo_config::Aggregation;
use apollo_domain::{Classification, Frame, FrameScan, ItemState, Task, TaskState};

/// Whether an item has reached a terminal state.
pub(crate) fn item_terminal(state: ItemState) -> bool {
    matches!(state, ItemState::Completed | ItemState::Failed)
}

/// The task state implied by its items: `Completed` once every item is terminal.
///
/// Retained as a single source of truth for the task-completion invariant; the
/// scheduler currently sets the terminal state directly as it finishes each task.
#[allow(dead_code)]
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
