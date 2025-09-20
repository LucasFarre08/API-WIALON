# Dumps do banco (telemetria_db)

Esta pasta armazena backups (`.sql`) do MySQL gerados via `mysqldump`.
Cada arquivo segue o padrão: `telemetria_db_YYYYMMDD_HHmmss.sql`.

## Como gerar
- Execute o script `dump_mysql_and_push.bat` na raiz do repositório.
  - Ele cria um novo dump com timestamp
  - Atualiza o arquivo `telemetria_db_latest.sql` (se configurado)
  - Faz `git pull --rebase`, `git add`, `git commit` e `git push`.

## Política de retenção
- Mantemos apenas os **N** dumps mais recentes (ex.: 7), definidos no script.
- Ajuste a variável `KEEP` no `.bat` para alterar.

## Como restaurar
No host com MySQL instalado:

## Observações
- Não versionamos `.sql` fora desta pasta.
- Se os arquivos crescerem muito, usar **Git LFS** para `.sql`.
- Nunca commitar credenciais reais no repositório (usar `.env` e `.env.example`).
