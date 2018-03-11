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
use self::time::{Duration, PreciseTime};
use super::runnable::Runnable;

#[derive(PartialEq, Debug)]
pub enum TaskState {
	Unstarted,
	Yielded,
	Completed,
	Error,
}

#[derive(PartialEq, Debug)]
pub struct Task<T>
where
	T: Generator + ?Sized
{
	/// Task identifier
	pub task_id: i32,

	/// The current state of this task. It would be one of the states
	/// defined by TaskState
	pub state: TaskState,

	/// Time in microseconds consumed by this task. This information
	/// is used by the scheduler for making scheduling decisions.
	pub time_consumed: i64,

	/// Code provided by the client to be executed in response to the
	/// request. This code is in the form of Generator so it can be
	/// paused and resumed.
	pub code_to_execute: T,
}

impl<T> Task<T>
where
	T: Generator + ?Sized
{
	/// This function creates a new Task. It takes task identifier,
	/// and client-provided code as a generator.
	pub fn new(task_id: i32, code_to_execute: Box<T>) -> Task<Box<T>> {
		let task = Task {
			task_id: task_id,
			state: TaskState::Unstarted,
			time_consumed: 0,
			code_to_execute: code_to_execute, 
		};
		task
	}

	/// This function is called by the scheduler to execute the task.
	/// It also measures the time consumed by the task until it either
	/// yielded or completed.
	/// Return value:
	///		0:	Task yielded
	///		1:	Task completed
	pub fn execute_code(&mut self) {
		let start = PreciseTime::now();
		let task_state = self.code_to_execute.resume();
		let end: Duration = start.to(PreciseTime::now());
		self.time_consumed += end.num_microseconds().expect("ERROR: Duration overflow!");

		match task_state {
			GeneratorState::Yielded(_i32) => self.state = TaskState::Yielded,
			GeneratorState::Complete(_i32) => self.state = TaskState::Completed,
     	}
	}
}

impl<T> Runnable for Task<T>
	where T: Generator + ?Sized
{
	fn run(&mut self) {
		self.execute_code();
	}

	fn get_state(&self) -> &TaskState {
		return &self.state;
	}
}

/// Unit tests for each method in Task struct
#[cfg(test)]
mod tests {
    use super::*;

    /// Tests whether the task was created successfully
    /// using Task::new().
    #[test]
    fn new_creates_task() {
        let gen = || {
			println!("Yielding...");
			yield 1;
			println!("Resumed");
			println!("Hello");
			println!("Ended.");
			return 1;
		};

		let mut task = Task::new(0, Box::new(gen));

		// The fact that task.task_id is readable means
		// that task was successfully created.
    	assert_eq!(task.task_id, 0);
    }

    /// Tests whether the execute_code() yields correctly.
    #[test]
    fn execute_code_function_yields() {
    	let gen = || {
			println!("Yielding...");
			yield 1;
			println!("Resumed");
			println!("Hello");
			println!("Ended.");
			return 1;
		};

		let mut task = Task::new(0, Box::new(gen));
		task.execute_code();

		assert_eq!(task.state, TaskState::Yielded);
    }

    /// Tests whether the execute_code() completes correctly.
    #[test]
    fn execute_code_function_completes() {
    	let gen = || {
			println!("Yielding...");
			yield 1;
			println!("Resumed");
			println!("Hello");
			println!("Ended.");
			return 1;
		};

		let mut task = Task::new(0, Box::new(gen));

		task.execute_code();
		task.execute_code();

		assert_eq!(task.state, TaskState::Completed);
    }

    /// Tests whether the execute_code() updates
    /// time_consumed field.
    #[test]
    fn execute_code_function_updates_time_consumed() {
    	let gen = || {
			println!("Yielding...");
			yield 1;
			println!("Resumed");
			println!("Hello");
			println!("Ended.");
			return 1;
		};

		let mut task = Task::new(0, Box::new(gen));
		task.execute_code();

		assert_ne!(task.time_consumed, 0);
    }

    /// Tests whether the get_state() returns state.
    #[test]
    fn get_state_works() {
    	let gen = || {
			println!("Yielding...");
			yield 1;
			println!("Resumed");
			println!("Hello");
			println!("Ended.");
			return 1;
		};

		let mut task = Task::new(0, Box::new(gen));
		
		assert_eq!(*task.get_state(), TaskState::Unstarted);
    }

    /// Tests whether run() calls execute_code().
    #[test]
    fn run_calls_execute_code() {
    	let gen = || {
			println!("Yielding...");
			yield 1;
			println!("Resumed");
			println!("Hello");
			println!("Ended.");
			return 1;
		};
		let mut task = Task::new(0, Box::new(gen));
		task.run();

		assert_eq!(task.state, TaskState::Yielded);
    }
}
