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

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};

use super::cycles;
use super::rpc;
use super::task::Task;
use super::task::TaskPriority;
use super::task::TaskState::*;

use e2d2::common::EmptyMetadata;
use e2d2::headers::IpHeader;
use e2d2::interface::Packet;

use spin::RwLock;

/// The maximum number of tasks the dispatcher can take in one go.
const MAX_RX_PACKETS: usize = 32;

/// Interval in microsecond which each task can use as credit to perform CPU work.
/// Under load shedding, the task which used more than this credit will be pushed-back.
const CREDIT_LIMIT_US: f64 = 0.5f64;

/// A simple round robin scheduler for Tasks in Sandstorm.
pub struct RoundRobin {
    // The time-stamp at which the scheduler last ran. Required to identify whether there is an
    // uncooperative task running on the scheduler.
    latest: AtomicUsize,

    // Atomic flag indicating whether there is a malicious/long running procedure on this
    // scheduler. If true, the scheduler must return down to Netbricks on the next call to poll().
    compromised: AtomicBool,

    // Identifier of the thread this scheduler is running on. Required for pre-emption.
    thread: AtomicUsize,

    // Identifier of the core this scheduler is running on. Required for pre-emption.
    core: AtomicIsize,

    // Run-queue of tasks waiting to execute. Tasks on this queue have either yielded, or have been
    // recently enqueued and never run before.
    waiting: RwLock<VecDeque<Box<Task>>>,

    // Response packets returned by completed tasks. Will be picked up and sent out the network by
    // the Dispatch task.
    responses: RwLock<Vec<Packet<IpHeader, EmptyMetadata>>>,

    // task_completed is incremented after the completion of each task. Reset to zero
    // after every 1M tasks.
    task_completed: RefCell<u64>,
}

// Implementation of methods on RoundRobin.
impl RoundRobin {
    /// Creates and returns a round-robin scheduler that can run tasks implementing the `Task`
    /// trait.
    ///
    /// # Arguments
    ///
    /// * `thread`: Identifier of the thread this scheduler will run on.
    /// * `core`:   Identifier of the core this scheduler will run on.
    pub fn new(thread: u64, core: i32) -> RoundRobin {
        RoundRobin {
            latest: AtomicUsize::new(cycles::rdtsc() as usize),
            compromised: AtomicBool::new(false),
            thread: AtomicUsize::new(thread as usize),
            core: AtomicIsize::new(core as isize),
            waiting: RwLock::new(VecDeque::new()),
            responses: RwLock::new(Vec::new()),
            task_completed: RefCell::new(0),
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
    pub fn responses(&self) -> Vec<Packet<IpHeader, EmptyMetadata>> {
        let mut responses = self.responses.write();
        return responses.drain(..).collect();
    }

    /// Appends a list of responses to the scheduler.
    ///
    /// # Arguments
    ///
    /// * `resps`: A vector of response packets parsed upto their IP headers.
    pub fn append_resps(&self, resps: &mut Vec<Packet<IpHeader, EmptyMetadata>>) {
        self.responses.write().append(resps);
    }

    /// Returns the time-stamp at which the latest scheduling decision was made.
    #[inline]
    pub fn latest(&self) -> u64 {
        self.latest.load(Ordering::Relaxed) as u64
    }

    /// Sets the compromised flag on the scheduler.
    #[inline]
    pub fn compromised(&self) {
        self.compromised.store(true, Ordering::Relaxed);
    }

    /// Returns the identifier of the thread this scheduler was configured to run on.
    #[inline]
    pub fn thread(&self) -> u64 {
        self.thread.load(Ordering::Relaxed) as u64
    }

    /// Returns the identifier of the core this scheduler was configured to run on.
    #[inline]
    pub fn core(&self) -> i32 {
        self.core.load(Ordering::Relaxed) as i32
    }

    /// Picks up a task from the waiting queue, and runs it until it either yields or completes.
    pub fn poll(&self) {
        let mut total_time: u64 = 0;
        let mut db_time: u64 = 0;
        let credit = (CREDIT_LIMIT_US / 1000000f64) * (cycles::cycles_per_second() as f64);

        // XXX: Trigger Pushback if the two dispatcher invocation is 20 us apart.
        let time_trigger: u64 = 2000 * credit as u64;
        let mut previous: u64 = 0;
        loop {
            // Set the time-stamp of the latest scheduling decision.
            let current = cycles::rdtsc();
            self.latest.store(current as usize, Ordering::Relaxed);

            // If the compromised flag was set, then return.
            if self.compromised.load(Ordering::Relaxed) {
                return;
            }

            // If there are tasks to run, then pick one from the head of the queue, and run it until it
            // either completes or yields back.
            let task = self.waiting.write().pop_front();

            if let Some(mut task) = task {
                let mut is_scheduler: bool = false;
                let mut queue_length: usize = 0;
                let mut difference: u64 = 0;
                match task.priority() {
                    TaskPriority::DISPATCH => {
                        is_scheduler = true;
                        queue_length = self.waiting.read().len();

                        // The time difference include the dispatcher time to account the native
                        // operations.
                        difference = current - previous;
                        previous = current;
                    }

                    _ => {}
                }

                if task.run().0 == COMPLETED {
                    // The task finished execution, check for request and response packets. If they
                    // exist, then free the request packet, and enqueue the response packet.
                    if let Some((req, res)) = unsafe { task.tear() } {
                        req.free_packet();
                        self.responses
                            .write()
                            .push(rpc::fixup_header_length_fields(res));
                    }
                    if cfg!(feature = "execution") {
                        total_time += task.time();
                        db_time += task.db_time();
                        let mut count = self.task_completed.borrow_mut();
                        *count += 1;
                        let every = 1000000;
                        if *count >= every {
                            info!("Total {}, DB {}", total_time / (*count), db_time / (*count));
                            *count = 0;
                            total_time = 0;
                            db_time = 0;
                        }
                    }
                } else {
                    // The task did not complete execution. EITHER add it back to the waiting list so that it
                    // gets to run again OR run the pushback mechanism. The pushback starts only after that
                    // dispatcher task execution. Trigger pushback:-
                    //
                    // if there are MAX_RX_PACKETS /4 yeilded tasks in the queue, OR
                    // if two dispatcher invocations are 2000 us apart, AND
                    // if the current dispatcher invocation received MAX_RX_PACKETS /4 new tasks.
                    if cfg!(feature = "pushback")
                        && is_scheduler == true
                        && (queue_length >= MAX_RX_PACKETS / 4 || difference > time_trigger)
                        && ((self.waiting.read().len() - queue_length) >= MAX_RX_PACKETS / 4)
                    {
                        for _i in 0..queue_length {
                            let mut yeilded_task = self.waiting.write().pop_front().unwrap();

                            // Compute Ranking/Credit on the go for each task to pushback
                            // some of the tasks whose rank/credit is more than the threshold.
                            if (yeilded_task.state() == YIELDED)
                                && ((yeilded_task.time() - yeilded_task.db_time()) > credit as u64)
                            {
                                yeilded_task.set_state(STOPPED);
                                if let Some((req, res)) = unsafe { yeilded_task.tear() } {
                                    req.free_packet();
                                    self.responses
                                        .write()
                                        .push(rpc::fixup_header_length_fields(res));
                                }
                            } else {
                                // Not fair with the already yielded tasks, as being pushed at the end.
                                self.waiting.write().push_back(yeilded_task);
                            }
                        }
                    }
                    self.waiting.write().push_back(task);
                }
            }
        }
    }
}

// RoundRobin uses atomics and RwLocks. Hence, it is thread-safe. Need to explicitly mark it as
// Send and Sync here because the compiler does not do so. This is because Packet contains a *mut
// MBuf which is not Send and Sync. Similarly, the compiler appears to be having trouble with the
// "Task" trait object.
unsafe impl Send for RoundRobin {}
unsafe impl Sync for RoundRobin {}
