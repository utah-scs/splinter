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

use std::sync::Arc;
use std::collections::HashMap;

use super::table::Table;

use spin::RwLock;

use sandstorm::common::{TableId, TenantId};

/// This type represents a tenant in Sandstorm. It helps uniquely identify
/// a tenant, and maintains a map of all the data tables belonging to a
/// particular tenant.
pub struct Tenant {
    /// A unique identifier for the tenant.
    id: TenantId,

    /// A map of all the data tables belonging to a tenant. Each data table
    /// has a unique identifier.
    tables: RwLock<HashMap<TableId, Arc<Table>>>,
}

// Implementation of methods on tenant.
impl Tenant {
    /// This method returns a new tenant. The caller must ensure that the passed
    /// in identifier is unique i.e there doesn't exist in the system another
    /// tenant with the same identifier.
    ///
    /// # Arguments
    ///
    /// * `id`: A unique identifier for the new tenant.
    ///
    /// # Return
    ///
    /// A `Tenant` representing a tenant in the system.
    pub fn new(id: TenantId) -> Tenant {
        Tenant {
            id: id,
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// This method returns the identifier for the tenant.
    ///
    /// # Return
    ///
    /// The identifier for the tenant.
    #[inline]
    pub fn id(&self) -> TenantId {
        self.id.clone()
    }

    /// This method creates a new table for the tenant. If a table with the
    /// passed in identifier already exists, then this method does nothing.
    ///
    /// # Arguments
    ///
    /// * `id`: A unique identifier for the new table.
    pub fn create_table(&self, table_id: u64) {
        // Acquire a write lock.
        let mut map = self.tables.write();

        // Insert a new table and return.
        map.insert(table_id, Arc::new(Table::default()));
    }

    /// This method returns a table belonging to the tenant if it exists.
    ///
    /// # Arguments
    ///
    /// * `table_id`: The identifier for the table to be returned.
    ///
    /// # Return
    ///
    /// An atomic reference counted handle to the table if it exists.
    pub fn get_table(&self, table_id: TableId) -> Option<Arc<Table>> {
        // Acquire a read lock.
        let map = self.tables.read();

        // Lookup on table_id and return.
        map.get(&table_id).and_then(| table | { Some(Arc::clone(&table)) })
    }
}
