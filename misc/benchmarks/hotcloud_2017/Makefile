.PHONY: run run-full clean

run: one-col-select uint32-sum cpp_array cpp_map
	@nodejs one-col-select-fast.js
	@echo
	@./one-col-select
	@echo

run-full: one-col-select uint32-sum cpp_array cpp_map
	@nodejs one-col-select-fast.js
	@echo
	@./one-col-select
	@echo
	@nodejs uint32-sum.js
	@echo
	@./uint32-sum
	@echo
	@nodejs array.js
	@echo
	@./cpp_array
	@echo
	@nodejs map.js
	@echo
	@./cpp_map
	@echo

cpp_array: cpp_array.cpp
	g++ -std=c++11 -O3 $< -o $@

cpp_map: cpp_map.cpp
	g++ -std=c++11 -O3 $< -o $@

uint32-sum: uint32-sum.cc common.h
	g++ -std=c++11 -O3 $< -o $@

one-col-select: one-col-select.cc common.h
	g++ -g -std=c++11 -O3 $< -o $@

multi-col-select: multi-col-select.cc common.h
	g++ -std=c++11 -O3 $< -o $@

clean:
	-rm uint32-sum cpp_array
