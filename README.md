# Comparer Rust

Ferramenta em Rust para verificar endereços de Ethereum em duas bases de dados SQLite.

A partir da versão atual, a leitura da tabela `addresses` é dividida em faixas
de `rowid` processadas em paralelo, cada qual abrindo sua própria conexão
SQLite. Isso evita o custo de consultas com `OFFSET` e aproveita melhor as
várias CPUs disponíveis.

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

Também é possível controlar o número de linhas processadas em cada faixa da
tabela `addresses` pelo argumento `--chunk-size` ou pela variável de ambiente
`CHUNK_SIZE`. O valor padrão é 50000.

## Licença

Este projeto está licenciado sob os termos da [licença MIT](LICENSE).
