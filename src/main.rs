use clap::Parser;
use rayon::prelude::*;
use rusqlite::{Connection, Result};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;

use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn verify_table_exists(conn: &Connection, table: &str, db_path: &str) -> Result<()> {
    let sql = format!("SELECT 1 FROM {} LIMIT 1", table);
    match conn.prepare(&sql).and_then(|mut stmt| stmt.exists([])) {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("❌ Table '{}' not found in {}", table, db_path);
            Err(e)
        }
    }
}

fn verify_addresses_table(path: &str) -> Result<()> {
    let conn = Connection::open(path)?;
    verify_table_exists(&conn, "addresses", path)
}

const DEFAULT_WALLETS_DB: &str = "E:\\rust\\address_checker\\wallets3.db";
const DEFAULT_ADDR_DB: &str = "E:\\rust\\get_addresses\\ethereum_addresses.db";
const CHUNK_SIZE: usize = 50000; // Processa 50k endereços por vez

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Caminho para o banco de wallets
    #[arg(long, env = "WALLETS_DB", default_value = DEFAULT_WALLETS_DB)]
    wallets_db: String,

    /// Caminho para o banco de endereços
    #[arg(long, env = "ADDR_DB", default_value = DEFAULT_ADDR_DB)]
    addr_db: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let t0 = Instant::now();

    println!("🚀 Iniciando verificação paralela de endereços...");

    // 1. Carrega apenas as wallets (dataset menor)
    let wallets = load_wallets(&args.wallets_db)?;
    println!("📊 Wallets carregadas: {} endereços", wallets.len());

    // 2. Verifica se a tabela de endereços existe
    verify_addresses_table(&args.addr_db)?;

    // 3. Processa em chunks sem saber o total
    let matches = process_all_chunks_streaming(&wallets, &args.addr_db)?;

    // 3. Relatório
    let dt = t0.elapsed().as_secs_f64();
    println!("🎯 Coincidências: {} | Tempo: {:.2}s", matches.len(), dt);

    // 4. Salva resultado
    if !matches.is_empty() {
        if let Err(e) = save_to_file(&matches) {
            eprintln!("⚠️  Falha ao salvar arquivo: {e}");
        }
    } else {
        println!("ℹ️  Nenhuma coincidência encontrada");
    }

    Ok(())
}

fn load_wallets(path: &str) -> Result<HashSet<String>> {
    let conn = Connection::open(path)?;
    verify_table_exists(&conn, "wallets", path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=OFF;
         PRAGMA temp_store=MEMORY;
         PRAGMA cache_size=-100000;",
    )?;

    let mut stmt = conn.prepare("SELECT address FROM wallets")?;
    let mut rows = stmt.query([])?;

    let mut wallets = HashSet::new();
    while let Some(row) = rows.next()? {
        // Normalize to lowercase to enable case-insensitive comparison
        wallets.insert(row.get::<_, String>(0)?.to_lowercase());
    }

    Ok(wallets)
}

fn process_all_chunks_streaming(wallets: &HashSet<String>, addr_db: &str) -> Result<Vec<String>> {
    let conn = Connection::open(addr_db)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=OFF;
         PRAGMA temp_store=MEMORY;
         PRAGMA cache_size=-25000;",
    )?;

    let mut all_matches = Vec::new();
    let mut last_rowid: i64 = 0;

    loop {
        let mut stmt = conn.prepare(
            "SELECT rowid, address FROM addresses WHERE rowid > ? ORDER BY rowid LIMIT ?",
        )?;
        let mut rows = stmt.query(rusqlite::params![last_rowid, CHUNK_SIZE as i64])?;

        let mut chunk_addresses = Vec::new();
        while let Some(row) = rows.next()? {
            last_rowid = row.get::<_, i64>(0)?;
            chunk_addresses.push(row.get::<_, String>(1)?);
        }

        if chunk_addresses.is_empty() {
            break;
        }

        let matches: Vec<String> = chunk_addresses
            .par_iter()
            .filter(|addr| wallets.contains(&addr.to_lowercase()))
            .cloned()
            .collect();

        all_matches.extend(matches);
    }

    Ok(all_matches)
}

use std::io;
fn save_to_file(addrs: &[String]) -> io::Result<()> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        .as_secs();

    let mut f = File::create(format!("coincidencias_{ts}.txt"))?;
    writeln!(f, "ENDEREÇOS ETHEREUM COINCIDENTES")?;
    writeln!(f, "Total: {}\n", addrs.len())?;

    for (i, addr) in addrs.iter().enumerate() {
        writeln!(f, "{}. {}", i + 1, addr)?;
    }

    println!("💾 Resultado salvo em coincidencias_{ts}.txt");
    Ok(())
}
