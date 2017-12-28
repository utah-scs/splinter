all:
	(cd db; cargo build)
	(cd ext/tao; cargo build)

.PHONY: so-test

so-test:
	(cd db; cargo build --release)
	(cd ext/tao; cargo build --release)
	$(foreach i,$(shell seq 0 99),cp ext/tao/target/release/deps/libtao.so ext/tao/target/release/deps/libtao$(i).so;)
	(cd db; RUST_BACKTRACE=1 cargo test -- ext_basic --nocapture)

bench:
	(cd db; cargo test --release scale_db_bench -- --nocapture)

run:
	(cd db; RUST_LOG=db cargo run -- --nocapture)
