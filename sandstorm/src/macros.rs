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

#[macro_export]
macro_rules! GET {
    ($db:ident, $table:ident, $key:ident, $obj:ident) => {
        let (server, found, val) = $db.search_get_in_cache($table, &$key);
        if server == false {
            if found == false {
                yield 0;
                $obj = $db.get($table, &$key);
            } else {
                $obj = val;
            }
        } else {
            $obj = $db.get($table, &$key);
        }
    };
}

/// TODO: Change it later, not implemented fully.
#[macro_export]
macro_rules! MULTIGET {
    ($db:ident, $table:ident, $keylen:ident, $keys:ident, $buf:ident) => {
        let mut is_server = false;
        let mut objs = Vec::new();
        for key in $keys.chunks($keylen as usize) {
            if key.len() != $keylen as usize {
                break;
            }
            let (server, _, val) = $db.search_get_in_cache($table, key);
            if server == true {
                $buf = $db.multiget($table, $keylen, &$keys);
                is_server = server;
                break;
            } else {
                objs.push(Bytes::from(val.unwrap().read()))
            }
        }
        if is_server == false {
            unsafe {
                $buf = Some(MultiReadBuf::new(objs));
            }
        }
    };
}
