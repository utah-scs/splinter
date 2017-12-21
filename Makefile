all:
	(cd db; cargo build)

bench:
	(cd db; cargo test --release scale_db_bench -- --nocapture)

run:
	(cd db; RUST_LOG=db cargo run)
