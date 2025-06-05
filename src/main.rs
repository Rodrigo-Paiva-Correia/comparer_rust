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

const DEFAULT_WALLETS_DB: &str = "E:\\rust\\address_checker\\wallets8.db";
const DEFAULT_ADDR_DB: &str = "E:\\rust\\get_addresses\\ethereum_addresses.db";
const DEFAULT_CHUNK_SIZE: usize = 50000; // Processa 50k endereços por vez

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Caminho para o banco de wallets
    #[arg(long, env = "WALLETS_DB", default_value = DEFAULT_WALLETS_DB)]
    wallets_db: String,

    /// Caminho para o banco de endereços
    #[arg(long, env = "ADDR_DB", default_value = DEFAULT_ADDR_DB)]
    addr_db: String,

    /// Tamanho dos blocos de leitura da tabela de endereços
    #[arg(long, env = "CHUNK_SIZE", default_value_t = DEFAULT_CHUNK_SIZE)]
    chunk_size: usize,
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

    // 3. Processa em chunks utilizando múltiplas conexões ao SQLite
    let matches = process_all_chunks_parallel(&wallets, &args.addr_db, args.chunk_size)?;

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

fn process_all_chunks_parallel(
    wallets: &HashSet<String>,
    addr_db: &str,
    chunk_size: usize,
) -> Result<Vec<String>> {
    // Descobre o maior rowid para determinar as faixas
    let max_rowid: i64 = {
        let conn = Connection::open(addr_db)?;
        conn.query_row("SELECT MAX(rowid) FROM addresses", [], |row| row.get(0))?
    };

    // Gera os limites iniciais de cada faixa
    let starts: Vec<i64> = (0..=max_rowid).step_by(chunk_size).collect();

    // Cada faixa é processada em paralelo, abrindo uma conexão própria
    let chunk_results: Result<Vec<Vec<String>>> = starts
        .into_par_iter()
        .map(|start| {
            let end = start + chunk_size as i64;
            let conn = Connection::open(addr_db)?;
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=OFF;
                 PRAGMA temp_store=MEMORY;
                 PRAGMA cache_size=-25000;",
            )?;

            let mut stmt = conn.prepare(
                "SELECT address FROM addresses WHERE rowid > ? AND rowid <= ? ORDER BY rowid",
            )?;
            let mut rows = stmt.query(rusqlite::params![start, end])?;

            let mut matches = Vec::new();
            while let Some(row) = rows.next()? {
                let addr: String = row.get(0)?;
                if wallets.contains(&addr.to_lowercase()) {
                    matches.push(addr);
                }
            }

            Ok(matches)
        })
        .collect();

    Ok(chunk_results?.into_iter().flatten().collect())
}

use std::io;
fn save_to_file(addrs: &[String]) -> io::Result<()> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
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
