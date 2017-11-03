#include <iostream>
#include <chrono>
#include <unordered_map>

using namespace std;

int main () {
	typedef std::chrono::high_resolution_clock Time;
	typedef std::chrono::milliseconds ms;
	typedef std::chrono::duration<float> fsec;

	unordered_map<long, double> umap;
	long i;
	for (i = 0; i < 10*1000*1000; i++) {
		umap.insert(make_pair(i, i + 0.1));
	}

	auto before = Time::now();
	double result = 0;
	for (i = 0; i < 10*1000*1000; i++) {
		result = result + umap.find(i)->second;
	}
	auto after = Time::now();
	fsec gap = after - before;
	ms m = std::chrono::duration_cast<ms>(gap);

	cout << m.count() << " " << result << endl;
	return 0;
}
