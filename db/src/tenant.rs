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

use std::collections::HashMap;

use super::table::Table;
use super::common::{TableId, TenantId};

/// This type represents a tenant in Sandstorm. It helps uniquely identify
/// a tenant, and maintains a map of all the data tables belonging to a
/// particular tenant.
pub struct Tenant {
    /// A unique identifier for the tenant.
    pub id: TenantId,

    /// A map of all the data tables belonging to a tenant. Each data table
    /// has a unique identifier.
    pub tables: HashMap<TableId, Table>,
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
            tables: HashMap::new(),
        }
    }

    /// This method creates a new table for the tenant. If a table with the
    /// passed in identifier already exists, then this method does nothing.
    ///
    /// # Arguments
    ///
    /// * `id`: A unique identifier for the new table.
    pub fn create_table(&mut self, table_id: u64) {
        self.tables.insert(table_id, Table::default());
    }
}
