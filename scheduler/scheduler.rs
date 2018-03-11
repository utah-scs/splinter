/* Copyright (c) 2017 University of Utah
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
 
extern crate time;

use std::ops::{Generator, GeneratorState};
use std::collections::VecDeque;
use self::time::{Duration, PreciseTime};
use super::runnable::Runnable;
use super::task::TaskState;

pub struct Scheduler
{
	/// FIFO queue of runnable (already constructed) tasks.
	/// This queue also contains a special Dispatch task whose
	/// job is to pick packets, construct tasks, and enqueue
	/// them onto this queue.
	pub run_q: VecDeque<Box<Runnable>>,
}

impl Scheduler
{
	/// This function creates a new scheduler.
	pub fn new() -> Scheduler {
		let scheduler = Scheduler {
			run_q: VecDeque::new(),
		};
		scheduler
	}

	/// This function starts the scheduler and enters an infinite loop.
	/// Then, it picks tasks from run_queue and executes them.
	pub fn start_scheduler(&mut self) {
		loop {	
			while !self.run_q.is_empty() {
				let mut task = match self.run_q.pop_front() {
		    		Some(t) => t,
		    		None => panic!(),
		    	};
		    	task.run();

		    	// What should be the behavior at Unstarted and Error state?
		    	match task.get_state() {
		    		&TaskState::Unstarted => println!("Undefined state"),
		    		
		    		// Task yielded. Enqueue it back so it can be
		    		// resumed later.
		    		&TaskState::Yielded => self.enqueue(task),

		    		// Later, this is where we would create and send
		    		// response packet.
		    		&TaskState::Completed => println!("Task completed."),

		    		&TaskState::Error => println!("Undefined error state."),
		    	}
			}
		}
	}

	/* This function enqueues the task at the back of queue.
	 Wouldn't it be faster if the caller directly push_back()'s
	 onto the queue without using this function?
	*/
	pub fn enqueue(&mut self, task: Box<Runnable>) {
		self.run_q.push_back(task);
	}
}

/// Unit tests for each method in Scheduler struct
#[cfg(test)]
mod tests {
    use super::*;
    use task::Task;

    /// Tests whether the scheduler was created successfully
    /// using Scheduler::new().
    #[test]
    fn new_creates_scheduler() {
		let mut scheduler = Scheduler::new();

		// The fact that task.task_id is readable means
		// that task was successfully created.
    	assert_eq!(scheduler.run_q.len(), 0);
    }

    /// Tests whether the scheduler enqueues tasks
    /// using Scheduler::enqueue().
    #[test]
    fn enqueue_works() {
		let mut scheduler = Scheduler::new();

		let gen = || {
			println!("Yielding...");
			yield 1;
			println!("Resumed");
			println!("Hello");
			println!("Ended.");
			return 1;
		};

		let mut task = Task::new(0, Box::new(gen));

		scheduler.enqueue(Box::new(task));

		assert_eq!(scheduler.run_q.len(), 1);
    }
}
