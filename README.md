# Comparer Rust

Ferramenta em Rust para verificar endereços de Ethereum em duas bases de dados SQLite.

## Uso

Os caminhos padrões para os bancos são utilizados caso nenhuma opção seja informada. Eles são:

- `WALLETS_DB`: `E:\rust\address_checker\wallets3.db`
- `ADDR_DB`: `E:\rust\get_addresses\ethereum_addresses.db`

É possível sobrescrever esses caminhos por variáveis de ambiente ou pelos argumentos de linha de comando `--wallets-db` e `--addr-db`.

Exemplo de execução informando explicitamente os caminhos:

```bash
cargo run --release -- \
    --wallets-db /caminho/para/wallets.db \
    --addr-db /caminho/para/addresses.db
```

Se preferir usar variáveis de ambiente:

```bash
export WALLETS_DB=/caminho/para/wallets.db
export ADDR_DB=/caminho/para/addresses.db
cargo run --release
```
