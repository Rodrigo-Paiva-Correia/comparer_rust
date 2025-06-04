use rayon::prelude::*;
use rusqlite::{Connection, Result};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub const CHUNK_SIZE: usize = 50000; // Processa 50k endereços por vez
pub const MAX_THREADS: usize = 4; // Limita threads simultâneas

pub fn load_wallets(wallets_db: &str) -> Result<HashSet<String>> {
    let conn = Connection::open(wallets_db)?;
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
        wallets.insert(row.get::<_, String>(0)?);
    }

    Ok(wallets)
}

pub fn process_all_chunks_streaming(wallets: &HashSet<String>, addr_db: &str) -> Result<Vec<String>> {
    let all_matches = Arc::new(Mutex::new(Vec::new()));
    let wallets_arc = Arc::new(wallets.clone());
    let processed_chunks = Arc::new(Mutex::new(0));

    let chunk_tasks: Vec<_> = (0..MAX_THREADS)
        .map(|thread_id| {
            let matches_clone = Arc::clone(&all_matches);
            let wallets_clone = Arc::clone(&wallets_arc);
            let processed_clone = Arc::clone(&processed_chunks);
            let addr_db = addr_db.to_string();

            std::thread::spawn(move || {
                let mut chunk_idx = thread_id;

                loop {
                    match process_single_chunk(chunk_idx, &wallets_clone, &addr_db) {
                        Ok(chunk_matches) => {
                            if chunk_matches.is_empty() {
                                break;
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

    for handle in chunk_tasks {
        if let Err(e) = handle.join() {
            eprintln!("⚠️  Thread falhou: {:?}", e);
        }
    }

    let final_matches = all_matches.lock().unwrap().clone();
    Ok(final_matches)
}

pub fn process_single_chunk(chunk_idx: usize, wallets: &HashSet<String>, addr_db: &str) -> Result<Vec<String>> {
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

    while let Some(row) = rows.next()? {
        chunk_addresses.push(row.get::<_, String>(0)?);
    }

    if chunk_addresses.is_empty() {
        return Ok(Vec::new());
    }

    let matches: Vec<String> = chunk_addresses
        .par_iter()
        .filter(|addr| wallets.contains(*addr))
        .cloned()
        .collect();

    Ok(matches)
}

pub fn save_to_file(addrs: &[String]) -> std::io::Result<()> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
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

