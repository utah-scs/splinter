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

use bincode::{deserialize, serialize};
use hashbrown::HashMap;

use std::cell::RefCell;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use rustlearn::ensemble::random_forest;
use rustlearn::linear_models::sgdclassifier;
use rustlearn::metrics;
use rustlearn::prelude::*;
use rustlearn::trees::decision_tree;

use super::common;

/// Return a 64-bit timestamp using the rdtsc instruction.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub fn rdtsc() -> u64 {
    unsafe {
        let lo: u32;
        let hi: u32;
        asm!("rdtsc" : "={eax}"(lo), "={edx}"(hi) : : : "volatile");
        (((hi as u64) << 32) | lo as u64)
    }
}

/// Store the model for each extension.
pub struct Model {
    /// This is used to store the serialized version of the model.
    pub serialized: Vec<u8>,

    /// This is used to store the deserialized version of the model.
    pub deserialized: sgdclassifier::SGDClassifier,
}

/// Add some methods to Model which can be used to use Model struct.
impl Model {
    /// This method returns a new object of Model struct.
    ///
    /// # Arguments
    ///
    /// * `serialized`: A vector of u8, which is serialized version of the ML model.
    ///
    /// # Return
    ///
    /// A new Model which stores the serialized version and deserialized version of
    /// a machine learning model.
    pub fn new(serialized: Vec<u8>) -> Model {
        Model {
            deserialized: deserialize(&serialized).unwrap(),
            serialized: serialized,
        }
    }
}

/// This method is used to add an entry to the thread local `GLOBAL_MODEL`.
///
/// # Arguments
///
/// * `name`: The name of extension which needs the ML model.
/// * `serialized`: The serialized version of the ML model.
pub fn insert_model(name: String, serialized: Vec<u8>) {
    GLOBAL_MODEL.with(|a_model| {
        let model = Model::new(serialized);
        (*a_model).borrow_mut().insert(name, Arc::new(model));
    });
}

/// Thread local Hash table which stores the mapping between extension and ml model.
thread_local!(pub static GLOBAL_MODEL: RefCell<HashMap<String, Arc<Model>>> = RefCell::new(HashMap::new()));

/// This method is used to add an entry to the static global `MODEL`.
///
/// # Arguments
///
/// * `name`: The name of extension which needs the ML model.
/// * `serialized`: The serialized version of the ML model.
pub fn insert_global_model(name: String, serialized: Vec<u8>) {
    let model = Model::new(serialized);
    MODEL.lock().unwrap().insert(name, Arc::new(model));
}

/// Global static Hash table which stores the mapping between extension and ml model.
lazy_static! {
    pub static ref MODEL: Mutex<HashMap<String, Arc<Model>>> = Mutex::new(HashMap::new());
}

/// Read and convert the content of a file to String and return the string to caller.
///
/// # Arguments
///
/// * `filename`: Name of the file content of which will be returned as a String.
///
/// # Return
/// The content of a file as a String.
pub fn get_raw_data(filename: &str) -> String {
    let path = Path::new(filename);

    let raw_data = match File::open(&path) {
        Err(_) => {
            panic!("Error in opening file {}", filename);
        }
        Ok(mut file) => {
            let mut file_data = String::new();
            file.read_to_string(&mut file_data).unwrap();
            file_data
        }
    };

    raw_data
}

/// Convert a long multiline string into a `SparseRowArray`. The format of one line in the
/// multiline string is fixed number of values separed by the space (ex: 1 2 3 4 5 6 7 8).
///
/// # Arguments
///
/// * `data`: The multiline string containing the data.
/// * `rows`: The number of rows in the multiline string.
/// * `cols`: The number of cols in the a single line in the multiline string.
///
/// # Return
/// A SparseRowArray of floats.
pub fn build_x_matrix(data: &str, rows: usize, cols: usize) -> SparseRowArray {
    let mut array = SparseRowArray::zeros(rows, cols);
    let mut row_num = 0;

    for (_row, line) in data.lines().enumerate() {
        let mut col_num = 0;
        for col_str in line.split_whitespace() {
            array.set(row_num, col_num, f32::from_str(col_str).unwrap());
            col_num += 1;
        }
        row_num += 1;
    }

    array
}

/// Convert a long multiline string into a `SparseColumnArray`. The format of one line in the
/// multiline string is fixed number of values separed by the space (ex: 1 2 3 4 5 6 7 8 9 10).
///
/// # Arguments
///
/// * `data`: The multiline string containing the data.
/// * `rows`: The number of rows in the multiline string.
/// * `cols`: The number of cols in the a single line in the multiline string.
///
/// # Return
/// A SparseColumnArray of floats.
pub fn build_col_matrix(data: &str, rows: usize, cols: usize) -> SparseColumnArray {
    let mut array = SparseColumnArray::zeros(rows, cols);
    let mut row_num = 0;

    for (_row, line) in data.lines().enumerate() {
        let mut col_num = 0;
        for col_str in line.split_whitespace() {
            array.set(row_num, col_num, f32::from_str(col_str).unwrap());
            col_num += 1;
        }
        row_num += 1;
    }

    array
}

/// This method converts a multiline string containing one element in each line to an Array.
///
/// # Arguments
///
/// * `data`: A multiline string.
///
/// # Return
/// An array.
pub fn build_y_array(data: &str) -> Array {
    let mut y = Vec::new();

    for line in data.lines() {
        for datum_str in line.split_whitespace() {
            let datum = datum_str.parse::<i32>().unwrap();
            y.push(datum);
        }
    }

    Array::from(y.iter().map(|&x| x as f32).collect::<Vec<f32>>())
}

/// This method get the file and convert the file to SparseRowArray. This method
/// is used to get the data to train and test the ML model.
pub fn get_train_data() -> (SparseRowArray, SparseRowArray) {
    let X_train = build_x_matrix(
        &get_raw_data(common::TRAINING_DATASET),
        common::TRAINING_ROWS,
        common::TRAINING_COLS,
    );
    let X_test = build_x_matrix(
        &get_raw_data(common::TESTING_DATASET),
        common::TESTING_ROWS,
        common::TESTING_COLS,
    );

    (X_train, X_test)
}

/// The supervised model needs the label for each record in the dataset, this method
/// gets the target data for training and testing dataset.
pub fn get_target_data() -> (Array, Array) {
    let y_train = build_y_array(&get_raw_data(common::TRAINING_TARGET));
    let y_test = build_y_array(&get_raw_data(common::TESTING_TARGET));

    (y_train, y_test)
}

/// This method runs the SGD classifier on the training dataset and also tests the
/// accuracy on the test dataset.
///
/// # Arguments
///
/// * `X_train`: The pointer to the SparseRowArray containing the training data.
/// * `X_test`: The pointer to the SparseRowArray containing the testing data.
/// * `y_train`: The pointer to the Array containing the target for training data.
/// * `y_test`: The pointer to the Array containing the target for testing data.
///
/// # Return
/// A serialized version of SGD model for the given dataset.
pub fn run_sgdclassifier(
    X_train: &SparseRowArray,
    X_test: &SparseRowArray,
    y_train: &Array,
    y_test: &Array,
) -> Vec<u8> {
    println!("Running SGDClassifier...");

    let num_epochs = 200;

    let mut model = sgdclassifier::Hyperparameters::new(X_train.cols()).build();

    for _ in 0..num_epochs {
        model.fit(X_train, y_train).unwrap();
    }

    let predictions = model.predict(X_test).unwrap();
    let accuracy = metrics::accuracy_score(y_test, &predictions);

    println!("SGDClassifier accuracy: {}%", accuracy * 100.0);

    let serialized = serialize(&model).unwrap();
    println!("Size of serialized model {} Bytes", serialized.len());
    let start = rdtsc();
    let model: sgdclassifier::SGDClassifier = deserialize(&serialized).unwrap();
    let diff1 = rdtsc() - start;

    let X = build_x_matrix(&get_raw_data("./../misc/data/positive.feat"), 1, 25);
    let Y = build_x_matrix(&get_raw_data("./../misc/data/negative.feat"), 1, 25);

    let start = rdtsc();
    let pos = model.predict(&X).unwrap().data()[0];
    let neg = model.predict(&Y).unwrap().data()[0];
    let diff2 = rdtsc() - start;

    println!("Positive {:#?}, Negative {:#?}", pos, neg);
    println!("CPU cycle for deserialization {}", diff1);
    println!("CPU cycle for 2 prediction {}\n", diff2);

    serialized
}

/// This method runs the Decision Tree classifier on the training dataset
/// and also tests the accuracy on the test dataset.
///
/// # Arguments
///
/// * `X_train`: The pointer to the SparseRowArray containing the training data.
/// * `X_test`: The pointer to the SparseRowArray containing the testing data.
/// * `y_train`: The pointer to the Array containing the target for training data.
/// * `y_test`: The pointer to the Array containing the target for testing data.
///
/// # Return
/// A serialized version of Decision Tree classifier model for the given dataset.
pub fn run_decision_tree(
    X_train: &SparseRowArray,
    X_test: &SparseRowArray,
    y_train: &Array,
    y_test: &Array,
) -> Vec<u8> {
    println!("Running DecisionTree...");

    let X_train = SparseColumnArray::from(X_train);
    let X_test = SparseColumnArray::from(X_test);

    let mut model = decision_tree::Hyperparameters::new(X_train.cols()).build();

    model.fit(&X_train, y_train).unwrap();

    let predictions = model.predict(&X_test).unwrap();
    let accuracy = metrics::accuracy_score(y_test, &predictions);

    println!("DecisionTree accuracy: {}%", accuracy * 100.0);

    let serialized = serialize(&model).unwrap();
    println!("Size of serialized model {} Bytes", serialized.len());
    let start = rdtsc();
    let model: decision_tree::DecisionTree = deserialize(&serialized).unwrap();
    let diff1 = rdtsc() - start;

    let X = build_col_matrix(&get_raw_data("./../misc/data/positive.feat"), 1, 25);
    let Y = build_col_matrix(&get_raw_data("./../misc/data/negative.feat"), 1, 25);
    let start = rdtsc();
    let pos = model.predict(&X).unwrap().data()[0];
    let neg = model.predict(&Y).unwrap().data()[0];
    let diff2 = rdtsc() - start;

    println!("Positive {:#?}, Negative {:#?}", pos, neg);
    println!("CPU cycle for deserialization {}", diff1);
    println!("CPU cycle for 2 prediction {}\n", diff2);

    serialized
}

/// This method runs the Random Forest classifier on the training dataset
/// and also tests the accuracy on the test dataset.
///
/// # Arguments
///
/// * `X_train`: The pointer to the SparseRowArray containing the training data.
/// * `X_test`: The pointer to the SparseRowArray containing the testing data.
/// * `y_train`: The pointer to the Array containing the target for training data.
/// * `y_test`: The pointer to the Array containing the target for testing data.
///
/// # Return
/// A serialized version of Random Forest classifier model for the given dataset.
pub fn run_random_forest(
    X_train: &SparseRowArray,
    X_test: &SparseRowArray,
    y_train: &Array,
    y_test: &Array,
) -> Vec<u8> {
    println!("Running RandomForest...");

    let num_trees = 20;

    let tree_params = decision_tree::Hyperparameters::new(X_train.cols());
    let mut model = random_forest::Hyperparameters::new(tree_params, num_trees).build();

    model.fit(X_train, y_train).unwrap();

    let predictions = model.predict(X_test).unwrap();
    let accuracy = metrics::accuracy_score(y_test, &predictions);

    println!("RandomForest accuracy: {}%", accuracy * 100.0);
    let serialized = serialize(&model).unwrap();
    println!("Size of serialized model {} Bytes", serialized.len());
    let start = rdtsc();
    let model: random_forest::RandomForest = deserialize(&serialized).unwrap();
    let diff1 = rdtsc() - start;

    let X = build_x_matrix(&get_raw_data("./../misc/data/positive.feat"), 1, 25);
    let Y = build_x_matrix(&get_raw_data("./../misc/data/negative.feat"), 1, 25);

    let start = rdtsc();
    let pos = model.predict(&X).unwrap().data()[0];
    let neg = model.predict(&Y).unwrap().data()[0];
    let diff2 = rdtsc() - start;

    println!("Positive {:#?}, Negative {:#?}", pos, neg);
    println!("CPU cycle for deserialization {}", diff1);
    println!("CPU cycle for 2 prediction {}\n", diff2);

    serialized
}

/// This method runs all the classifier on the give dataset and
/// returns the serialized version for each of the ML model.
///
/// # Return
/// Serialized version for each of the classifiers called in this function.
pub fn run_ml_application() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let (X_train, X_test) = get_train_data();
    let (y_train, y_test) = get_target_data();

    println!(
        "Training data: {} by {} matrix with {} nonzero entries",
        X_train.rows(),
        X_train.cols(),
        X_train.nnz()
    );
    println!(
        "Test data: {} by {} matrix with {} nonzero entries\n",
        X_test.rows(),
        X_test.cols(),
        X_test.nnz()
    );

    let sgd = run_sgdclassifier(&X_train, &X_test, &y_train, &y_test);
    let d_tree = run_decision_tree(&X_train, &X_test, &y_train, &y_test);
    let r_forest = run_random_forest(&X_train, &X_test, &y_train, &y_test);

    (sgd, d_tree, r_forest)
}
