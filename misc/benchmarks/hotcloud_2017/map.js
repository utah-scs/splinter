var a = [];

for (i = 0; i < 10*1000*1000; i++) {
	a.push(JSON.stringify({x: i + 0.1}));
}

var before = new Date().getTime();
var result = 0;

for (i = 0; i < 10*1000*1000; i++) {
	var obj = JSON.parse(a[i]);
	result = result + obj.x;
}

var after = new Date().getTime();
var diff = after - before;

console.log(result);
console.log(before);
console.log(after);
console.log(diff);

