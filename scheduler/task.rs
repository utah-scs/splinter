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

pub enum TaskState {
	Unstarted,
	Yielded,
	Completed,
	Error,
}

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
		self.time_consumed = end.num_microseconds().expect("ERROR: Duration overflow!");

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
