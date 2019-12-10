all: netbricks
	(cd db; cargo build --release)
	(cd splinter; cargo build --release)
	(cd ext/bad; cargo build --release)
	(cd ext/tao; cargo build --release)
	(cd ext/get; cargo build --release)
	(cd ext/put; cargo build --release)
	(cd ext/err; cargo build --release)
	(cd ext/long; cargo build --release)
	(cd ext/aggregate; cargo build --release)
	(cd ext/pushback; cargo build --release)
	(cd ext/scan; cargo build --release)
	(cd ext/analysis; cargo build --release)
	(cd ext/auth; cargo build --release)
	(cd ext/ycsbt; cargo build --release)
	(cd ext/checksum; cargo build --release)

.PHONY: so-test

so-test: netbricks
	(cd db; cargo build --release)
	(cd splinter; cargo build --release)
	(cd ext/test; cargo build --release)
	$(foreach i,$(shell seq 0 99),cp ext/test/target/release/libtest.so ext/test/target/release/libtest$(i).so;)
	(cd db; LD_LIBRARY_PATH=../net/target/native RUST_BACKTRACE=1 cargo run --release --bin ext_bench)
	(cd ext/test; cargo clean)

bench: netbricks
	(cd db; LD_LIBRARY_PATH=../net/target/native cargo run --release --bin table_bench)

run:
	(cd db; RUST_LOG=db cargo run -- --nocapture)

netbricks:
	(cd net/native; make)
	mkdir -p net/target/native
	cp net/native/libzcsi.so net/target/native/libzcsi.so

test: netbricks
	(cd ext/test; cargo build --release)
	(cd db; LD_LIBRARY_PATH=../net/target/native cargo test)
	(cd splinter; LD_LIBRARY_PATH=../net/target/native cargo test)
	(cd sandstorm; LD_LIBRARY_PATH=../net/target/native cargo test)

coverage: netbricks
	(curl -sL https://github.com/xd009642/tarpaulin/releases/download/0.7.0/cargo-tarpaulin-0.7.0-travis.tar.gz |\
	       tar xvz -C ${HOME}/.cargo/bin)
	(cd ext/test; cargo build --release)
	(cd db; LD_LIBRARY_PATH=../net/target/native cargo-tarpaulin)
	(cd splinter; LD_LIBRARY_PATH=../net/target/native cargo-tarpaulin)
	(cd sandstorm; LD_LIBRARY_PATH=../net/target/native cargo-tarpaulin)
	(rm -rf db/target/debug/; rm -rf splinter/target/debug/; rm -rf sandstorm/target/debug/)

clean:
	(cd db; cargo clean)
	(cd splinter; cargo clean)
	(cd ext/bad; cargo clean)
	(cd ext/tao; cargo clean)
	(cd ext/get; cargo clean)
	(cd ext/put; cargo clean)
	(cd ext/err; cargo clean)
	(cd ext/test; cargo clean)
	(cd ext/long; cargo clean)
	(cd ext/aggregate; cargo clean)
	(cd ext/pushback; cargo clean)
	(cd ext/scan; cargo clean)
	(cd ext/analysis; cargo clean)
	(cd ext/auth; cargo clean)
	(cd ext/ycsbt; cargo clean)
	(cd ext/checksum; cargo clean)
	(cd sandstorm; cargo clean)
	(cd net; ./build.sh clean)
	(cd util; cargo clean)
