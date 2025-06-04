use rayon::prelude::*;
use rusqlite::{Connection, Result};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
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
    let matches = process_all_chunks_streaming(&wallets)?;

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

fn load_wallets() -> Result<HashSet<String>> {
    let conn = Connection::open(WALLETS_DB)?;
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

fn process_all_chunks_streaming(wallets: &HashSet<String>) -> Result<Vec<String>> {
    let all_matches = Arc::new(Mutex::new(Vec::new()));
    let wallets_arc = Arc::new(wallets.clone());
    let processed_chunks = Arc::new(Mutex::new(0));

    // Processa chunks infinitamente até não haver mais dados
    let chunk_tasks: Vec<_> = (0..MAX_THREADS)
        .map(|thread_id| {
            let matches_clone = Arc::clone(&all_matches);
            let wallets_clone = Arc::clone(&wallets_arc);
            let processed_clone = Arc::clone(&processed_chunks);

            std::thread::spawn(move || {
                let mut chunk_idx = thread_id;

                loop {
                    match process_single_chunk(chunk_idx, &wallets_clone) {
                        Ok(chunk_matches) => {
                            if chunk_matches.is_empty() {
                                // Se chunk está vazio, pode ser fim dos dados
                                // Verifica se realmente não há mais dados
                                if let Ok(has_more) = check_has_more_data(chunk_idx) {
                                    if !has_more {
                                        break; // Fim dos dados
                                    }
                                }
                            }

                            // Adiciona matches encontrados
                            if !chunk_matches.is_empty() {
                                let mut global_matches = matches_clone.lock().unwrap();
                                global_matches.extend(chunk_matches.clone());
                                let total_matches = global_matches.len();
                                drop(global_matches);

                                let mut processed = processed_clone.lock().unwrap();
                                *processed += 1;
                                println!(
                                    "✅ Chunk {} processado - {} matches locais, {} total",
                                    chunk_idx,
                                    chunk_matches.len(),
                                    total_matches
                                );
                                drop(processed);
                            }

                            // Próximo chunk para esta thread
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

fn process_single_chunk(chunk_idx: usize, wallets: &HashSet<String>) -> Result<Vec<String>> {
    let conn = Connection::open(ADDR_DB)?;
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

    // Processa chunk em paralelo
    let matches: Vec<String> = chunk_addresses
        .par_iter()
        .filter(|addr| wallets.contains(*addr))
        .cloned()
        .collect();

    Ok(matches)
}

fn check_has_more_data(chunk_idx: usize) -> Result<bool> {
    let conn = Connection::open(ADDR_DB)?;
    let offset = chunk_idx * CHUNK_SIZE;

    let mut stmt = conn.prepare(&format!(
        "SELECT 1 FROM addresses LIMIT 1 OFFSET {}",
        offset
    ))?;

    let mut rows = stmt.query([])?;
    Ok(rows.next()?.is_some())
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
