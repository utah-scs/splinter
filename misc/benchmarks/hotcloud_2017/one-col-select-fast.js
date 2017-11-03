//bench = require('./bench.js')
var startTime = new Date();

var report = function (testName, bytes) {
  var end = new Date();
  var ms = end.getTime() - startTime.getTime();
  console.log(testName)
  console.log(' Language JS')
  console.log(' Runtime ' + ms + ' ms');
  console.log(' Throughput ' + (bytes / 1024 / 1024 / (ms / 1000)) + ' MB/s');
}

var fieldSize = 4; // sizeof(Float64)
var fieldsPerRow = 16;
var recordSize = fieldsPerRow * fieldSize;
// Must be strictly less than 1 GB.
var bytes = 1 * 1024 * 1024 * 1024 - 1;
var nRows = Math.floor(bytes / recordSize);

//var buf = new ArrayBuffer(bytes);
//var a = new DataView(buf);
var a = new Uint32Array(nRows * fieldsPerRow);
var out = new Uint32Array(nRows);
var outn = 0;

for (var i = 0; i < nRows; ++i) {
  for (var j = 0; j < fieldsPerRow; ++j) {
    //a.setUint32(i * recordSize + j * fieldSize, Math.floor(Math.random() * 100));
    a[i * fieldsPerRow + j] = Math.floor(Math.random() * 100);
  }
  out[i] = 0;
}

// Schema: A: uint32_t
// Query: select sum(A) from T
function q1() {
  var s = 0;
  for (var i = 0; i < nRows; ++i) {
    s += a[i * fieldsPerRow];
  }
  out[outn++] = s;
}

// Schema: A: uint32_t
// Query: select sum(A) from T where A < x
function q2(x) {
  var s = 0;
  for (var i = 0; i < nRows; ++i) {
    //var v = a.getUint32(i * recordSize);
    var v = a[i * fieldsPerRow];
    if (v < x) {
      s += v;
    }
  }
  out[outn++] = s;
}

// Schema: A: uint32_t
// Query: select sum(A) from T where A < x
function q3(x) {
  for (var i = 0; i < nRows; ++i) {
    //var v = a.getUint32(i * recordSize);
    var v = a[i * fieldsPerRow];
    if (v < x) {
      out[outn++] = v + v;
    }
  }
}

// Schema: A: uint32_t
// Query: select A + B + C + D from T where A < x
function q4(x) {
  for (var i = 0; i < nRows; ++i) {
    var off = i * fieldsPerRow;
    var ar = a[off++];
    var br = a[off++];
    var cr = a[off++];
    var dr = a[off++];
    if (ar < x) {
      out[outn++] = ar + br + cr + dr;
    }
  }
}

function q5(x) {
  var s = 0;
  for (var i = 0; i < nRows; ++i) {
    var off = i * fieldsPerRow;
    var ar = a[off++];
    var br = a[off++];
    var cr = a[off++];
    var dr = a[off++];
    if (ar < x) {
      s += Math.pow(ar, Math.pow(br, Math.pow(cr * dr)));
    }
  }
  out[outn++] = s;
}

startTime = new Date();
q1();
report('OneColSelect::q1', bytes);
outn = 0;

for (var iter = 0; iter < 10; ++iter) {
  for (var selectivity = 0; selectivity < 101; selectivity += 10) {
    startTime = new Date();
    q2(selectivity);
    report('OneColSelect::q2(' + selectivity + ')', bytes);
    outn = 0;

    startTime = new Date();
    q3(selectivity);
    report('OneColSelect::q3(' + selectivity + ')', bytes);
    outn = 0;

    startTime = new Date();
    q4(selectivity);
    report('OneColSelect::q4(' + selectivity + ')', bytes);
    outn = 0;

    startTime = new Date();
    q5(selectivity);
    report('OneColSelect::q5(' + selectivity + ')', bytes);
    outn = 0;
  }
}
