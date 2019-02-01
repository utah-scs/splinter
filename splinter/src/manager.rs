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

use std::rc::Rc;
use std::sync::Arc;

use super::container::Container;
use super::dispatch::*;
use super::proxy::ProxyDB;

use db::master::Master;
use db::task::{Task, TaskPriority, TaskState, TaskState::*};

use sandstorm::common::TenantId;

pub struct TaskManager {
    // This is used for RPC requests and extensions lookup in tables.
    tenant: u32,

    // This is used to parse the request for the arguments. The request is `name+arguments`, so
    // this length can be used to split the payload in name and arguments.
    name_length: u32,

    // The packet/buffer consisting of the RPC request header and payload
    // that invoked the extension. This is required to potentially pass in
    // arguments to an extension. For example, a get() extension might require
    // a key and table identifier to be passed in.
    payload: Arc<Vec<u8>>,

    // Identifier for each request, which is the timestamp for the packet generation.
    // This is used to identify native requests associated with an extension.
    id: u64,

    // The reference to the task generator, which is used to suspend/resume the generator.
    task: Vec<Box<Task>>,

    // A ref counted pointer to a master service. The master service
    // implements the primary interface to the database.
    master: Arc<Master>,
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
    pub fn new(
        master_service: Arc<Master>,
        req: &[u8],
        tenant_id: u32,
        name_len: u32,
        timestamp: u64,
    ) -> TaskManager {
        TaskManager {
            tenant: tenant_id,
            name_length: name_len,
            payload: Arc::new(req.to_vec()),
            id: timestamp,
            task: Vec::with_capacity(1),
            master: master_service,
        }
    }

    /// This method returns the unique id, which was used for the request.
    pub fn get_id(&self) -> u64 {
        self.id.clone()
    }

    /// This method returns the payload used in the request.
    fn get_payload(&self) -> &[u8] {
        &self.payload
    }

    /// This method creates a task for the extension on the client-side and add
    /// it to the task-manager.
    ///
    /// # Arguments
    /// * `sender_service`: A reference to the service which helps in the RPC request generation.
    pub fn create_generator(&mut self, sender_service: Arc<Sender>) {
        let tenant_id: TenantId = self.tenant as TenantId;
        let name_length: usize = self.name_length as usize;

        // Read the extension's name from the request payload.
        let mut name = Vec::new();
        name.extend_from_slice(self.get_payload().split_at(name_length).0);
        let name: String = String::from_utf8(name).expect("ERROR: Failed to get ext name.");

        if let Some(ext) = self.master.extensions.get(tenant_id, &name) {
            let db = Rc::new(ProxyDB::new(
                self.tenant,
                self.id,
                Arc::clone(&self.payload),
                self.name_length as usize,
                sender_service,
            ));
            self.task
                .push(Box::new(Container::new(TaskPriority::REQUEST, db, ext)));
        } else {
            info!("Unable to create a generator for this request");
        }
    }

    /// This method updates the RW set for the extension.
    ///
    /// # Arguments
    /// * `records`: A reference to the RWset sent back by the server when the extension is
    ///             pushed back.
    pub fn update_rwset(&mut self, records: &[u8]) {
        for record in records.chunks(131) {
            self.task[0].update_cache(record);
        }
    }

    /// This method run the task associated with an extension. And on the completion
    /// of the task, it tear downs the task.
    ///
    /// # Return
    ///
    /// The taskstate on the completion, yielding, or waiting of the task.
    pub fn execute_task(&mut self) -> (TaskState, u64) {
        let task = self.task.pop();
        let mut taskstate: TaskState = INITIALIZED;
        let mut time: u64 = 0;
        if let Some(mut task) = task {
            if task.run().0 == COMPLETED {
                taskstate = task.state();
                time = task.time();
                unsafe {
                    task.tear();
                }
            // Do something for commit(Transaction commit?)
            } else {
                taskstate = task.state();
                time = task.time();
                self.task.push(task);
            }
        }
        (taskstate, time)
    }
}
