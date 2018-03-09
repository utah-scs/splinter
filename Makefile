all: netbricks
	(cd db; cargo build --release)
	(cd ext/tao; cargo build --release)
	(cd ext/get; cargo build --release)
	(cd ext/put; cargo build --release)

.PHONY: so-test

so-test: netbricks
	(cd db; cargo build --release)
	(cd ext/tao; cargo build --release)
	$(foreach i,$(shell seq 0 99),cp ext/tao/target/release/deps/libtao.so ext/tao/target/release/deps/libtao$(i).so;)
	(cd db; RUST_BACKTRACE=1 cargo run --release --bin ext_bench)

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
	(cd ext/tao; cargo clean)
	(cd ext/get; cargo clean)
	(cd sandstorm; cargo clean)
