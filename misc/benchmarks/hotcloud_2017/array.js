var a = [];
for (i = 0; i < 10*1000*1000; i++) {
	a.push(i+0.1);
}

var before = new Date().getTime();
var result = 0;

for (i = 0; i < 10*1000*1000; i++) {
	result = result + a[i];
}

var after = new Date().getTime();
var diff = after - before;

console.log(result);
console.log(before);
console.log(after);
console.log(diff);
