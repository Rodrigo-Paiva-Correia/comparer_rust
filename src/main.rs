use rayon::prelude::*;
use rusqlite::{Connection, Result};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc, Mutex,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const WALLETS_DB: &str = "E:\\rust\\address_checker\\wallets3.db";
const ADDR_DB: &str = "E:\\rust\\get_addresses\\ethereum_addresses.db";
const CHUNK_SIZE: usize = 50000; // Processa 50k endereços por vez
const MAX_THREADS: usize = 4; // Limita threads simultâneas

fn main() -> Result<()> {
    let t0 = Instant::now();

    println!("🚀 Iniciando verificação paralela de endereços...");

    // 1. Carrega apenas as wallets (dataset menor)
    let wallets = load_wallets()?;
    println!("📊 Wallets carregadas: {} endereços", wallets.len());

    // 2. Processa em chunks sem saber o total
    let matches = process_all_chunks_streaming(Arc::clone(&wallets))?;

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

fn load_wallets() -> Result<Arc<HashSet<String>>> {
    let conn = Connection::open(WALLETS_DB)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; 
         PRAGMA synchronous=OFF; 
         PRAGMA temp_store=MEMORY; 
         PRAGMA cache_size=-100000;",
    )?;

    // Determine wallet count to preallocate HashSet
    let count: usize = conn.query_row("SELECT COUNT(*) FROM wallets", [], |r| r.get(0))?;

    let mut stmt = conn.prepare("SELECT address FROM wallets")?;
    let mut rows = stmt.query([])?;

    let mut wallets = HashSet::with_capacity(count);
    while let Some(row) = rows.next()? {
        wallets.insert(row.get::<_, String>(0)?);
    }

    Ok(Arc::new(wallets))
}

fn process_all_chunks_streaming(wallets: Arc<HashSet<String>>) -> Result<Vec<String>> {
    let all_matches = Arc::new(Mutex::new(Vec::new()));
    let processed_chunks = Arc::new(Mutex::new(0));
    let next_rowid = Arc::new(AtomicI64::new(0));

    // Processa chunks infinitamente até não haver mais dados
    let chunk_tasks: Vec<_> = (0..MAX_THREADS)
        .map(|thread_id| {
            let matches_clone = Arc::clone(&all_matches);
            let wallets_clone = Arc::clone(&wallets);
            let processed_clone = Arc::clone(&processed_chunks);
            let rowid_counter = Arc::clone(&next_rowid);

            std::thread::spawn(move || {
                let conn = match Connection::open(ADDR_DB) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("⚠️  Erro ao abrir conexão na thread {}: {}", thread_id, e);
                        return;
                    }
                };
                if let Err(e) = conn.execute_batch(
                    "PRAGMA journal_mode=WAL;\n         PRAGMA synchronous=OFF;\n         PRAGMA temp_store=MEMORY;\n         PRAGMA cache_size=-25000;",
                ) {
                    eprintln!("⚠️  Erro aplicando PRAGMAs na thread {}: {}", thread_id, e);
                    return;
                }

                loop {
                    let start_rowid = rowid_counter.fetch_add(CHUNK_SIZE as i64, Ordering::SeqCst);

                    match process_single_chunk(start_rowid, &wallets_clone, &conn) {
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
                                "✅ Rowid {} processado - {} matches, {} total",
                                start_rowid,
                                chunk_matches.len(),
                                total_matches
                            );
                            drop(processed);
                        }
                        Err(e) => {
                            eprintln!("⚠️  Erro no rowid {}: {}", start_rowid, e);
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

fn process_single_chunk(
    start_rowid: i64,
    wallets: &HashSet<String>,
    conn: &Connection,
) -> Result<Vec<String>> {
    let end_rowid = start_rowid + CHUNK_SIZE as i64;

    let mut stmt = conn
        .prepare("SELECT address FROM addresses WHERE rowid >= ? AND rowid < ? ORDER BY rowid")?;

    let mut rows = stmt.query(rusqlite::params![start_rowid, end_rowid])?;
    // Preallocate vector for chunk addresses to reduce reallocations
    let mut chunk_addresses = Vec::with_capacity(CHUNK_SIZE);

    // Carrega apenas um chunk pequeno na memória
    while let Some(row) = rows.next()? {
        let addr: String = row.get(0)?;
        chunk_addresses.push(addr);
    }

    // Se chunk está vazio, retorna vazio
    if chunk_addresses.is_empty() {
        return Ok(Vec::new());
    }

    // Processa chunk em paralelo
    let matches: Vec<String> = chunk_addresses
        .par_iter()
        .filter(|addr| wallets.contains(*addr))
        .cloned()
        .collect();

    Ok(matches)
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
