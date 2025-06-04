use clap::Parser;
use rayon::prelude::*;
use rusqlite::{Connection, Result};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
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
const MAX_THREADS: usize = 4; // Limita threads simultâneas

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
    let all_matches = Arc::new(Mutex::new(Vec::new()));
    let wallets_arc = Arc::new(wallets.clone());
    let addr_db_arc = Arc::new(addr_db.to_string());
    let processed_chunks = Arc::new(Mutex::new(0));

    // Processa chunks infinitamente até não haver mais dados
    let chunk_tasks: Vec<_> = (0..MAX_THREADS)
        .map(|thread_id| {
            let matches_clone = Arc::clone(&all_matches);
            let wallets_clone = Arc::clone(&wallets_arc);
            let processed_clone = Arc::clone(&processed_chunks);
            let db_clone = Arc::clone(&addr_db_arc);

            std::thread::spawn(move || {
                let mut chunk_idx = thread_id;

                loop {
                    match process_single_chunk(chunk_idx, &wallets_clone, db_clone.as_str()) {
                        Ok(chunk_matches) => {
                            if chunk_matches.is_empty() {
                                break; // Fim dos dados
                            }

                            let mut global_matches = matches_clone.lock().unwrap();
                            global_matches.extend(chunk_matches.clone());
                            let total_matches = global_matches.len();
                            drop(global_matches);

                            let mut processed = processed_clone.lock().unwrap();
                            *processed += 1;
                            println!(
                                "✅ Chunk {} processado - {} matches, {} total",
                                chunk_idx,
                                chunk_matches.len(),
                                total_matches
                            );
                            drop(processed);

                            chunk_idx += MAX_THREADS;
                        }
                        Err(e) => {
                            eprintln!("⚠️  Erro no chunk {}: {}", chunk_idx, e);
                            break;
                        }
                    }
                }
            })
        })
        .collect();

    // Espera todas as threads terminarem
    for handle in chunk_tasks {
        if let Err(e) = handle.join() {
            eprintln!("⚠️  Thread falhou: {:?}", e);
        }
    }

    // Retorna resultado final
    let final_matches = all_matches.lock().unwrap().clone();
    Ok(final_matches)
}

fn process_single_chunk(chunk_idx: usize, wallets: &HashSet<String>, addr_db: &str) -> Result<Vec<String>> {
    let conn = Connection::open(addr_db)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=OFF;
         PRAGMA temp_store=MEMORY;
         PRAGMA cache_size=-25000;",
    )?;

    let offset = chunk_idx * CHUNK_SIZE;
    let mut stmt = conn.prepare(&format!(
        "SELECT address FROM addresses LIMIT {} OFFSET {}",
        CHUNK_SIZE, offset
    ))?;

    let mut rows = stmt.query([])?;
    let mut chunk_addresses = Vec::new();

    // Carrega apenas um chunk pequeno na memória
    while let Some(row) = rows.next()? {
        chunk_addresses.push(row.get::<_, String>(0)?);
    }

    // Se chunk está vazio, retorna vazio
    if chunk_addresses.is_empty() {
        return Ok(Vec::new());
    }

    // Processa chunk em paralelo com comparação case-insensitive
    let matches: Vec<String> = chunk_addresses
        .par_iter()
        .filter(|addr| wallets.contains(&addr.to_lowercase()))
        .cloned()
        .collect();

    Ok(matches)
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
