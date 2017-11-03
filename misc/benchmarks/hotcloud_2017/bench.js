var startTime = new Date();

var report = function (testName, bytes) {
  var end = new Date();
  var ms = end.getTime() - startTime.getTime();
  console.log(testName)
  console.log(' Language JS')
  console.log(' Runtime ' + ms + ' ms');
  console.log(' Throughput ' + (bytes / 1024 / 1024 / (ms / 1000)) + ' MB/s');
}

var start = function () {
  startTime = new Date();
}

module.exports = {
  report: report,
  start: start
};
