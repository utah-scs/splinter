var date1 = new Date().getMilliseconds();

for (count = 0; count < 1000000; count++) {
        null_func();
}

var date2 = new Date().getMilliseconds();
print("Call CPP cost:", date2 - date1, "ns");
