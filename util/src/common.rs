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

/// Path to the Training dataset for ML model.
pub const TRAINING_DATASET: &str = "./../misc/data/train.csv";
/// Path to the Training target for ML model.
pub const TRAINING_TARGET: &str = "./../misc/data/target.csv";
/// Number of rows in the Training dataset.
pub const TRAINING_ROWS: usize = 68411;
/// Number of columns in the Training dataset.
pub const TRAINING_COLS: usize = 25;

/// Path to the Testing dataset for ML model.
pub const TESTING_DATASET: &str = "./../misc/data/train.csv";
/// Path to the Testing target for ML model.
pub const TESTING_TARGET: &str = "./../misc/data/target.csv";
/// Number of rows in the Testing dataset.
pub const TESTING_ROWS: usize = 68411;
/// Number of columns in the Testing dataset.
pub const TESTING_COLS: usize = 25;
