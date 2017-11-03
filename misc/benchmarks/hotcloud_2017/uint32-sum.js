bench = require('./bench.js')

var c = 128 * 1024 * 1024;
var it = 8;
var sizeofUint32 = 4

var bytes = c * it * sizeofUint32;

var a = new Uint32Array(c);

for (var i = 0; i < c; ++i) {
  a[i] = i;
}

var start = new Date();

var s = 0;
for (var j = 0; j < it; ++j) {
  for (var i = 0; i < c; ++i) {
    s += a[i];
  }
}

var end = new Date();

bench.report('Uint32ArraySum', bytes, start, end);
