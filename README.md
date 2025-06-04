# Comparer Rust

This tool compares addresses in two SQLite databases and reports matches.

## Usage

Build the project and run the binary. Optional flags allow overriding the paths
of the wallets and addresses databases:

```bash
cargo run -- --wallet-db /path/to/wallets.db --addr-db /path/to/addresses.db
```

If a flag is not provided, the program falls back to the default paths defined
in the source.
