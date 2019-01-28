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
    tenant: u32,
    name_length: u32,

    // The packet/buffer consisting of the RPC request header and payload
    // that invoked the extension. This is required to potentially pass in
    // arguments to an extension. For example, a get() extension might require
    // a key and table identifier to be passed in.
    payload: Arc<Vec<u8>>,

    // Identifier for each request, which is the timestamp for the packet generation.
    // This is used to identify native requests associated with an extension.
    id: u64,

    //
    task: Vec<Box<Task>>,

    //
    master: Arc<Master>,

    //
    sender: Arc<Sender>,
}

impl TaskManager {
    pub fn new(
        master_service: Arc<Master>,
        sender_service: Arc<Sender>,
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
            sender: sender_service,
        }
    }

    pub fn get_id(&self) -> u64 {
        self.id.clone()
    }

    fn get_payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn create_generator(&mut self) {
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
                Arc::clone(&self.sender),
            ));
            self.task
                .push(Box::new(Container::new(TaskPriority::REQUEST, db, ext)));
        } else {
            info!("Unable to create a generator for this request");
        }
    }

    pub fn execute_task(&mut self) -> TaskState {
        let task = self.task.pop();
        let mut taskstate: TaskState = INITIALIZED;
        if let Some(mut task) = task {
            if task.run().0 == COMPLETED {
                //Do something
                taskstate = task.state();
            } else {
                taskstate = task.state();
                self.task.push(task);
            }
        }
        taskstate
    }
}
