/* Copyright (c) 2019 University of Utah
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

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::Arc;

use super::container::Container;
use super::dispatch::*;
use super::proxy::ProxyDB;

use db::master::Master;
use db::task::{Task, TaskPriority, TaskState, TaskState::*};

use sandstorm::common::TenantId;
use util::model::GLOBAL_MODEL;

/// TaskManager handles the information for a pushed-back extension on the client side.
pub struct TaskManager {
    // A ref counted pointer to a master service. The master service
    // implements the primary interface to the database.
    master: Arc<Master>,

    // The reference to the task generator, which is used to suspend/resume the generator.
    ready: VecDeque<Box<Task>>,

    ///  The HashMap containing the waiting tasks.
    pub waiting: HashMap<u64, Box<Task>>,
}

impl TaskManager {
    /// This function creates and returns the TaskManager for the pushed back task. The client uses
    /// this object to resume the extension on the client side.
    ///
    /// # Arguments
    ///
    /// * `master_service`: A reference to a Master which will be used to construct tasks from received
    ///                    response.
    /// * `req`: A reference to the request sent by the client, it will be helpful in task creation
    ///          if the requested is pushed back.
    /// * `tenant_id`: Tenant id will be needed reuqest generation.
    /// * `name_len`: This will be useful in parsing the request and find out the argument for consecutive requests.
    /// * `timestamp`: This is unique-id for the request and consecutive requests will have same id.
    ///
    /// # Return
    ///
    /// A TaskManager for generator creation and task execution on the client.
    pub fn new(master_service: Arc<Master>) -> TaskManager {
        TaskManager {
            master: master_service,
            ready: VecDeque::with_capacity(32),
            waiting: HashMap::with_capacity(32),
        }
    }

    /// This method creates a task for the extension on the client-side and add
    /// it to the task-manager.
    ///
    /// # Arguments
    /// * `sender_service`: A reference to the service which helps in the RPC request generation.
    pub fn create_task(
        &mut self,
        id: u64,
        req: &[u8],
        tenant: u32,
        name_length: usize,
        sender_service: Arc<Sender>,
    ) {
        let tenant_id: TenantId = tenant as TenantId;
        let name_length: usize = name_length as usize;

        // Read the extension's name from the request payload.
        let mut name = Vec::new();
        name.extend_from_slice(req.split_at(name_length).0);
        let name: String = String::from_utf8(name).expect("ERROR: Failed to get ext name.");

        // Get the model for the given extension.
        let mut model = None;
        // If the extension doesn't need an ML model, don't waste CPU cycles in lookup.
        if cfg!(feature = "ml-model") {
            GLOBAL_MODEL.with(|a_model| {
                if let Some(a_model) = (*a_model).borrow().get(&name) {
                    model = Some(Arc::clone(a_model));
                }
            });
        }

        if let Some(ext) = self.master.extensions.get(tenant_id, name) {
            let db = Rc::new(ProxyDB::new(
                tenant,
                id,
                req.to_vec(),
                name_length as usize,
                sender_service,
                model,
            ));
            self.waiting.insert(
                id,
                Box::new(Container::new(TaskPriority::REQUEST, db, ext, id)),
            );
        } else {
            info!("Unable to create a generator for this request");
        }
    }

    /// Delete a waiting task from the scheduler.
    ///
    /// # Arguments
    /// *`id`: The unique identifier for the task.
    pub fn delete_task(&mut self, id: u64) {
        if self.waiting.remove(&id).is_none() == true {
            info!("No task to delete with id {}", id);
        }
    }

    /// Find the number of tasks waiting in the ready queue.
    ///
    /// # Return
    ///
    /// The current length of the task queue.
    pub fn get_queue_len(&self) -> usize {
        self.ready.len()
    }

    /// This method updates the RW set for the extension.
    ///
    /// # Arguments
    /// * `records`: A reference to the RWset sent back by the server when the extension is
    ///             pushed back.
    pub fn update_rwset(&mut self, id: u64, records: &[u8], recordlen: usize, keylen: usize) {
        if let Some(mut task) = self.waiting.remove(&id) {
            if cfg!(feature = "checksum") {
                let (keys, records) = records.split_at(377);
                task.update_cache(keys, 8);
                for record in records.chunks(recordlen) {
                    task.update_cache(record, keylen);
                }
            } else {
                for record in records.chunks(recordlen) {
                    task.update_cache(record, keylen);
                }
            }
            self.ready.push_back(task);
        } else {
            info!("No waiting task with id {}", id);
        }
    }

    /// This method run the task associated with an extension. And on the completion
    /// of the task, it tear downs the task.
    ///
    /// # Return
    ///
    /// The taskstate on the completion, yielding, or waiting of the task.
    pub fn execute_task(&mut self) -> (TaskState, u64) {
        let task = self.ready.pop_front();
        let mut taskstate: TaskState = INITIALIZED;
        let mut time: u64 = 0;
        if let Some(mut task) = task {
            if task.run().0 == COMPLETED {
                taskstate = task.state();
                time = task.time();
                unsafe {
                    task.tear();
                    // Do something for commit(Transaction commit?)
                }
            } else {
                taskstate = task.state();
                time = task.time();
                if taskstate == YIELDED {
                    self.ready.push_back(task);
                } else if taskstate == WAITING {
                    self.waiting.insert(task.get_id(), task);
                }
            }
        }
        (taskstate, time)
    }
}
