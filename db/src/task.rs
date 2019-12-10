/* Copyright (c) 2018 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

use e2d2::common::EmptyMetadata;
use e2d2::headers::UdpHeader;
use e2d2::interface::Packet;

/// This enum represents the different states a task can be in.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq)]
pub enum TaskState {
    /// A task is in this state when it has just been created, but has not
    /// had a chance to execute on the CPU yet.
    INITIALIZED = 0x01,

    /// A task is in this state when it is currently running on the CPU.
    RUNNING = 0x02,

    /// A task is in this state when it has got a chance to run on the CPU at
    /// least once, but has yeilded to the scheduler, and is currently not
    /// executing on the CPU.
    YIELDED = 0x03,

    /// A task is in this state when it has finished executing completely, and
    /// it's results are ready.
    COMPLETED = 0x04,

    /// A task is in this state when it has been stopped without completion, after
    /// setting this state, the pushback mechanism will run.
    STOPPED = 0x5,

    /// A task is in this state when it has been suspended due to IO. On the client side
    /// the task can wait for the native operation responses.
    WAITING = 0x6,
}

/// This enum represents the priority of a task in the system. A smaller value
/// indicates a task with a higher priority.
#[repr(u8)]
#[derive(Clone, PartialEq)]
pub enum TaskPriority {
    /// The priority of a dispatch task. Highest in the system, because this
    /// task is responsible for all network processing.
    DISPATCH = 0x01,

    /// The priority of a task corresponding to an RPC request.
    REQUEST = 0x02,
}

/// This trait consists of methods that will allow a type to be run as a task
/// on Sandstorm's scheduler.
pub trait Task {
    /// When called, this method should "run" the task.
    ///
    /// # Return
    ///
    /// A tuple whose first member consists of the current state of the task
    /// (`TaskState`), and whose second member consists of the amount of time
    /// in cycles the task continuously ran for during this call to run().
    fn run(&mut self) -> (TaskState, u64);

    /// When called, this method should return the current state of the task.
    ///
    /// # Return
    ///
    /// The current state of the task (`TaskState`).
    fn state(&self) -> TaskState;

    /// When called, this method should return the total time for which the task
    /// has run since it was created.
    ///
    /// # Return
    ///
    /// The total time for which the task has run in cycles.
    fn time(&self) -> u64;

    /// When called, this method should return the total time for which the task
    /// has spent in db operations since it was created.
    ///
    /// # Return
    ///
    /// The total time for which the task has spent in db operations in cycles.
    fn db_time(&self) -> u64;

    /// When called, this method should return the priority of the task.
    ///
    /// # Return
    ///
    /// The priority of the task.
    fn priority(&self) -> TaskPriority;

    /// When called, this method should return any packets or buffers that were passed in during
    /// creation. This method shoulf be called when a task has completed or aborted.
    ///
    /// # Return
    ///
    /// A tuple whose first member consists of the request packet, and whose
    /// second member consists of a response packet, if available.
    unsafe fn tear(
        &mut self,
    ) -> Option<(
        Packet<UdpHeader, EmptyMetadata>,
        Packet<UdpHeader, EmptyMetadata>,
    )>;

    /// When called, this method will change the task state to `state` and will return.
    ///
    /// # Arguments
    ///
    /// * `state`: The state, which will be assigned to the task.
    fn set_state(&mut self, state: TaskState);

    /// This method is called after the pushback and updates the local RW set on the client
    /// side.
    ///
    /// # Arguments
    ///
    /// * `record`: The record, which will be added to the RW set.
    fn update_cache(&mut self, record: &[u8], keylen: usize);

    /// This method returns the unique task identifier.
    ///
    /// # Return
    ///
    /// Task id to identifiy the task uniquely.
    fn get_id(&self) -> u64;
}
