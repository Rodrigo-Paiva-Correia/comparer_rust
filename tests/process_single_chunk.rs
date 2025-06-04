use comparer_rust::{load_wallets, process_single_chunk};
use rusqlite::Connection;
use tempfile::NamedTempFile;

#[test]
fn test_process_single_chunk_matches() -> rusqlite::Result<()> {
    // wallets db
    let wallets_temp = NamedTempFile::new().unwrap();
    let wallets_path = wallets_temp.path();
    let conn = Connection::open(wallets_path)?;
    conn.execute_batch("CREATE TABLE wallets (address TEXT);")?;
    conn.execute("INSERT INTO wallets (address) VALUES (?1)", ["0xabc"])?;
    conn.execute("INSERT INTO wallets (address) VALUES (?1)", ["0xdef"])?;

    drop(conn);

    // addresses db
    let addr_temp = NamedTempFile::new().unwrap();
    let addr_path = addr_temp.path();
    let conn2 = Connection::open(addr_path)?;
    conn2.execute_batch("CREATE TABLE addresses (address TEXT);")?;
    conn2.execute("INSERT INTO addresses (address) VALUES (?1)", ["0x000"])?;
    conn2.execute("INSERT INTO addresses (address) VALUES (?1)", ["0xabc"])?;
    conn2.execute("INSERT INTO addresses (address) VALUES (?1)", ["0xdef"])?;

    drop(conn2);

    // load wallets
    let wallets = load_wallets(wallets_path.to_str().unwrap())?;
    let mut matches = process_single_chunk(0, &wallets, addr_path.to_str().unwrap())?;
    matches.sort();

    assert_eq!(matches, vec!["0xabc".to_string(), "0xdef".to_string()]);

    Ok(())
}
