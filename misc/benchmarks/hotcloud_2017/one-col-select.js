bench = require('./bench.js')

/*
var singleColumn = false;
if (singleColumn) {
  var nRows  = 256 * 1024 * 1024 - 1;
  var sizeofUint32 = 4;

  var bytes = nRows * sizeofUint32;

  var a;
  var out;
  var outn;

  var setup = function () {
    a = new Uint32Array(nRows);
    out = new Uint32Array(nRows);
    for (var i = 0; i < nRows; ++i) {
      a[i] = Math.floor(Math.random() * 100);
      out[i] = 0;
    }
    outn = 0;
  };

  var get = function (row, col) {
    return a[row];
  }

  var emit = function (value) {
    out[outn++] = value;
  }

  var reset = function () {
    outn = 0;
  }
} else {
*/
  var fieldSize = 4; // sizeof(Float64)
  var fieldsPerRow = 16;
  var recordSize = fieldsPerRow * fieldSize;
  // Must be strictly less than 1 GB.
  var bytes = 1 * 1024 * 1024 * 1024 - 1;
  var nRows = bytes / recordSize;

  var buf = new ArrayBuffer(bytes);
  var a = new DataView(buf);
  var out = new Uint32Array(nRows);
  var outn;

  var get = function (row, col) {
    return a.getUint32(recordSize * row + fieldSize * col);
  }

  var emit = function (value) {
    out[outn++] = value;
  }

  var set = function (row, col, value) {
    a.setUint32(recordSize * row + fieldSize * col, value);
  }

  var setup = function () {
    for (var i = 0; i < nRows; ++i) {
      for (var j = 0; j < fieldsPerRow; ++j) {
        set(i, j, Math.floor(Math.random() * 100));
      }
      out[i] = 0;
    }
    outn = 0;
  }

  var reset = function () {
    outn = 0;
  }
/*
}
*/

// Schema: A: uint32_t
// Query: select sum(A) from T
function q1() {
  var s = 0;
  for (var i = 0; i < nRows; ++i) {
    s += get(i, 0);
  }
  emit(s);
}

// Schema: A: uint32_t
// Query: select sum(A) from T where A < x
function q2(x) {
  var s = 0;
  for (var i = 0; i < nRows; ++i) {
    var v = get(i, 0);
    if (v < x) {
      s += v;
    }
  }
  emit(s);
}

// Schema: A: uint32_t
// Query: select sum(A) from T where A < x
function q3(x) {
  for (var i = 0; i < nRows; ++i) {
    var v = get(i, 0);
    if (v < x) {
      emit(v + v);
    }
  }
}

// Schema: A: uint32_t
// Query: select A + B + C + D from T where A < x
function q4(x) {
  for (var i = 0; i < nRows; ++i) {
    var a = get(i, 0);
    var b = get(i, 1);
    var c = get(i, 2);
    var d = get(i, 3);
    if (a < x) {
      emit(a + b + c + d);
    }
  }
}

function q5(x) {
  var s = 0;
  for (var i = 0; i < nRows; ++i) {
    var a = get(i, 0);
    var b = get(i, 1);
    var c = get(i, 2);
    var d = get(i, 3);
    if (a < x) {
      s += Math.pow(a, Math.pow(b, Math.pow(c * d)));
    }
  }
  emit(s);
}


//engine.setup();
setup();

bench.start();
q1();
bench.report('OneColSelect::q1', bytes);
reset();

for (var iter = 0; iter < 10; ++iter) {
  for (var selectivity = 0; selectivity < 101; selectivity += 10) {
    bench.start();
    q2(selectivity);
    bench.report('OneColSelect::q2(' + selectivity + ')', bytes);
    reset();

    bench.start();
    q3(selectivity);
    bench.report('OneColSelect::q3(' + selectivity + ')', bytes);
    reset();

    //if (!singleColumn) {
      bench.start();
      q4(selectivity);
      bench.report('OneColSelect::q4(' + selectivity + ')', bytes);
      reset();

      bench.start();
      q5(selectivity);
      bench.report('OneColSelect::q5(' + selectivity + ')', bytes);
      reset();
    //}
  }
}

// select sum(A + B) from T
// select A + B from T
// select A + B from T where A < 10
