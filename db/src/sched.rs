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

use std::cell::Cell;
use std::collections::VecDeque;

use super::task::Task;
use super::task::TaskState::*;

use e2d2::interface::Packet;
use e2d2::headers::UdpHeader;
use e2d2::common::EmptyMetadata;

use spin::RwLock;
use time::PreciseTime;

/// A simple round robin scheduler for Tasks in Sandstorm.
pub struct RoundRobin {
    // The time-stamp at which the scheduler last ran. Required to identify whether there is an
    // uncooperative task running on the scheduler.
    latest: Cell<PreciseTime>,

    // Run-queue of tasks waiting to execute. Tasks on this queue have either yielded, or have been
    // recently enqueued and never run before.
    waiting: RwLock<VecDeque<Box<Task>>>,

    // Response packets returned by completed tasks. Will be picked up and sent out the network by
    // the Dispatch task.
    responses: RwLock<Vec<Packet<UdpHeader, EmptyMetadata>>>,
}

// Implementation of methods on RoundRobin.
impl RoundRobin {
    /// Creates and returns a round-robin scheduler that can run tasks implementing the `Task`
    /// trait.
    pub fn new() -> RoundRobin {
        RoundRobin {
            latest: Cell::new(PreciseTime::now()),
            waiting: RwLock::new(VecDeque::new()),
            responses: RwLock::new(Vec::new()),
        }
    }

    /// Enqueues a task onto the scheduler. The task is enqueued at the end of the schedulers
    /// queue.
    ///
    /// # Arguments
    ///
    /// * `task`: The task to be added to the scheduler. Must implement the `Task` trait.
    #[inline]
    pub fn enqueue(&self, task: Box<Task>) {
        self.waiting.write().push_back(task);
    }

    /// Enqueues multiple tasks onto the scheduler.
    ///
    /// # Arguments
    ///
    /// * `tasks`: A deque of tasks to be added to the scheduler. These tasks will be run in the
    ///            order that they are provided in, and must implement the `Task` trait.
    #[inline]
    pub fn enqueue_many(&self, mut tasks: VecDeque<Box<Task>>) {
        self.waiting.write().append(&mut tasks);
    }

    /// Dequeues all waiting tasks from the scheduler.
    ///
    /// # Return
    ///
    /// A deque of all waiting tasks in the scheduler. This tasks might be in various stages of
    /// execution. Some might have run for a while and yielded, and some might have never run
    /// before. If there are no tasks waiting to run, then an empty vector is returned.
    #[inline]
    pub fn dequeue_all(&self) -> VecDeque<Box<Task>> {
        let mut tasks = self.waiting.write();
        return tasks.drain(..).collect();
    }

    /// Returns a list of pending response packets.
    ///
    /// # Return
    ///
    /// A vector of response packets that were returned by tasks that completed execution. This
    /// packets should be sent out the network. If there are no pending responses, then an empty
    /// vector is returned.
    #[inline]
    pub fn responses(&self) -> Vec<Packet<UdpHeader, EmptyMetadata>> {
        let mut responses = self.responses.write();
        return responses.drain(..).collect();
    }

    /// Returns the time-stamp at which the latest scheduling decision was made.
    #[inline]
    pub fn latest(&self) -> PreciseTime {
        self.latest.get()
    }

    /// Picks up a task from the waiting queue, and runs it until it either yields or completes.
    pub fn poll(&self) {
        // Set the time-stamp of the latest scheduling decision.
        self.latest.set(PreciseTime::now());

        // If there are tasks to run, then pick one from the head of the queue, and run it until it
        // either completes or yields back.
        let task = self.waiting.write().pop_front();

        if let Some(mut task) = task {
            if task.run().0 == COMPLETED {
                // The task finished execution, check for request and response packets. If they
                // exist, then free the request packet, and enqueue the response packet.
                if let Some((req, res)) = unsafe { task.tear() } {
                    req.free_packet();
                    self.responses.write().push(res);
                }
            } else {
                // The task did not complete execution. Add it back to the waiting list so that it
                // gets to run again.
                self.waiting.write().push_back(task);
            }
        }
    }
}
