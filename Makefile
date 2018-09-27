all: netbricks
	(cd db; cargo build --release)
	(cd ext/bad; cargo build --release)
	(cd ext/tao; cargo build --release)
	(cd ext/get; cargo build --release)
	(cd ext/put; cargo build --release)
	(cd ext/err; cargo build --release)
	(cd ext/long; cargo build --release)
	(cd ext/aggregate; cargo build --release)

.PHONY: so-test

so-test: netbricks
	(cd db; cargo build --release)
	(cd ext/test; cargo build --release)
	$(foreach i,$(shell seq 0 99),cp ext/test/target/release/deps/libtest.so ext/get/target/release/deps/libtest$(i).so;)
	(cd db; LD_LIBRARY_PATH=../net/target/native RUST_BACKTRACE=1 cargo run --release --bin ext_bench)

bench: netbricks
	(cd db; cargo run --release --bin table_bench)

run:
	(cd db; RUST_LOG=db cargo run -- --nocapture)

netbricks:
	(cd net/native; make)
	mkdir -p net/target/native
	cp net/native/libzcsi.so net/target/native/libzcsi.so

clean:
	(cd db; cargo clean)
	(cd ext/bad; cargo clean)
	(cd ext/tao; cargo clean)
	(cd ext/get; cargo clean)
	(cd ext/put; cargo clean)
	(cd ext/err; cargo clean)
	(cd ext/test; cargo clean)
	(cd ext/long; cargo clean)
	(cd sandstorm; cargo clean)
	(cd net; ./build.sh clean)
