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

use std::rc::Rc;
use std::sync::Arc;
use std::cell::Cell;
use std::ops::{Generator, GeneratorState};

use super::ext::Extension;
use super::context::Context;
use super::task::TaskState::*;
use super::common::PACKET_UDP_LEN;
use super::task::{Task, TaskPriority, TaskState};

use e2d2::interface::Packet;
use e2d2::headers::UdpHeader;
use e2d2::common::EmptyMetadata;

use sandstorm::db::DB;
use time::{Duration, PreciseTime};

/// A container for untrusted code that can be scheduled by the database.
pub struct Container {
    // The current state of the task. Required to determine if the task
    // has completed execution.
    state: TaskState,

    // The priority of the task. Required to determine when the task should
    // be run next, if it has not completed already.
    priority: TaskPriority,

    // The total amount of time the task has run for. Required to determine
    // when the task should be run next, and for accounting purposes.
    time: Duration,

    // An execution context for the task that implements the DB trait. Required
    // for the task to interact with the database.
    db: Cell<Option<Rc<Context>>>,

    // A handle to the dynamically loaded extension. Required to initialize the
    // task, and ensure that the extension stays loaded for as long as the task
    // is executing in the system.
    ext: Arc<Extension>,

    // The actual generator/coroutine containing the extension's code to be
    // executed inside the database.
    gen: Box<Generator<Yield = u64, Return = u64>>,
}

// Implementation of methods on Container.
impl Container {
    /// Creates a new container holding an untrusted extension that can be
    /// scheduled by the database.
    ///
    /// # Arguments
    ///
    /// * `prio`:    The priority of the container/task. Required by the
    ///              scheduler.
    /// * `context`: The execution context for the extension. Allows the
    ///              extension to interact with the database.
    /// * `ext`:     A handle to the extension that will be run inside this
    ///              container.
    ///
    /// # Return
    ///
    /// A container that when scheduled, runs the extension.
    pub fn new(prio: TaskPriority, context: Rc<Context>, ext: Arc<Extension>) -> Container {
        // The generator is initialized to a dummy. The first call to run() will
        // retrieve the actual generator from the extension.
        Container {
            state: INITIALIZED,
            priority: prio,
            time: Duration::microseconds(0),
            db: Cell::new(Some(context)),
            ext: ext,
            gen: Box::new(|| {
                yield 0;
                return 0;
            }),
        }
    }
}

// Implementation of the Task trait for Container.
impl Task for Container {
    /// Refer to the Task trait for Documentation.
    fn run(&mut self) -> (TaskState, Duration) {
        let start = PreciseTime::now();

        // If the task has never run before, retrieve the generator for the
        // extension first.
        if self.state == INITIALIZED {
            let context = self.db.replace(None).unwrap();
            self.gen = self.ext.get(Rc::clone(&context) as Rc<DB>);
            self.db.set(Some(context));
        }

        // Resume the task if need be. The task needs to be run/resumed only
        // if it is in the INITIALIZED or YIELDED state. Nothing needs to be
        // done if it has already completed, or was aborted.
        if self.state == INITIALIZED || self.state == YIELDED {
            self.state = RUNNING;

            match self.gen.resume() {
                GeneratorState::Yielded(_) => {
                    self.state = YIELDED;
                }

                GeneratorState::Complete(_) => {
                    self.state = COMPLETED;
                }
            }
        }

        // Calculate the amount of time the task executed for.
        let exec = start.to(PreciseTime::now());

        // Update the total execution time of the task.
        self.time = exec + self.time;

        // Return the state and the amount of time the task executed for.
        return (self.state, exec);
    }

    /// Refer to the Task trait for Documentation.
    fn state(&self) -> TaskState {
        self.state.clone()
    }

    /// Refer to the Task trait for Documentation.
    fn time(&self) -> Duration {
        self.time.clone()
    }

    /// Refer to the Task trait for Documentation.
    fn priority(&self) -> TaskPriority {
        self.priority.clone()
    }

    /// Refer to the Task trait for Documentation.
    unsafe fn tear(
        &mut self,
    ) -> Option<(
        Packet<UdpHeader, EmptyMetadata>,
        Packet<UdpHeader, EmptyMetadata>,
    )> {
        // First, drop the generator. Doing so ensures that self.db is the
        // only reference to the extension's execution context.
        self.gen = Box::new(|| {
            yield 0;
            return 0;
        });

        // Next, unwrap the execution context, and, retrieve and return the
        // request and response packets.
        let context = self.db.replace(None).unwrap();
        match Rc::try_unwrap(context) {
            Ok(db) => {
                let (req, res) = db.commit();

                let req = req.deparse_header(PACKET_UDP_LEN as usize);
                let res = res.deparse_header(PACKET_UDP_LEN as usize);

                return Some((req, res));
            }

            Err(_) => {
                panic!("Failed to unwrap context!");
            }
        }
    }
}
