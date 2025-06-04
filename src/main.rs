use comparer_rust::{load_wallets, process_all_chunks_streaming, save_to_file};
use rusqlite::Result;
use std::time::Instant;

const WALLETS_DB: &str = "E:\\rust\\address_checker\\wallets3.db";
const ADDR_DB: &str = "E:\\rust\\get_addresses\\ethereum_addresses.db";

fn main() -> Result<()> {
    let t0 = Instant::now();

    println!("🚀 Iniciando verificação paralela de endereços...");

    // 1. Carrega apenas as wallets (dataset menor)
    let wallets = load_wallets(WALLETS_DB)?;
    println!("📊 Wallets carregadas: {} endereços", wallets.len());

    // 2. Processa em chunks sem saber o total
    let matches = process_all_chunks_streaming(&wallets, ADDR_DB)?;

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
