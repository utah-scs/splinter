bench = require('./bench.js')

var fieldSize = 4; // sizeof(Float64)
var fieldsPerRow = 16;
var recordSize = fieldsPerRow * fieldSize;
// Must be strictly less than 1 GB.
var bytes = 1 * 1024 * 1024 * 1024 - 1;
var c = bytes / recordSize;

var buf = new ArrayBuffer(bytes);
var a = new DataView(buf);
var out = new Uint32Array(c);

var get = function (row, col) {
  return a.getUint32(recordSize * row + fieldSize * col);
}

var set = function (row, col, value) {
  a.setUint32(recordSize * row + fieldSize * col, value);
}

for (var i = 0; i < c; ++i) {
  for (var j = 0; i < fieldsPerRow; ++i) {
    set(i, j, Math.floor(Math.random() * 100));
  }
  out[i] = 0;
}

// Schema: A: uint32_t
// Query: select sum(A) from T
function q1() {
  var s = 0;
  for (var i = 0; i < c; ++i) {
    s += get(i, 0);
  }
}


// Schema: A: uint32_t
// Query: select sum(A) from T where A < x
function q2(x) {
  var s = 0;
  for (var i = 0; i < c; ++i) {
    if (get(i, 0) < x) {
      s += get(i, 0);
    }
  }
}

// Schema: A: uint32_t
// Query: select sum(A) from T where A < x
function q3(x, out) {
  var outn = 0;
  for (var i = 0; i < c; ++i) {
    if (get(i, 0) < x) {
      out[outn++] = get(i, 0) + get(i, 1);
    }
  }
}

var trials = 1;
for (var i = 0; i < trials; ++i) {
  var start = new Date();
  q1();
  var end = new Date();
  bench.report('MultiColSelect::q1', bytes, start, end);

  var start = new Date();
  q2(10 * i);
  var end = new Date();
  bench.report('MultiColSelect::q2', bytes, start, end);

  var start = new Date();
  q3(10 * i, out);
  var end = new Date();
  bench.report('MultiColSelect::q3', bytes, start, end);
}
