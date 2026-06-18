
# WackyWacky

![Banner WackyWacky](assets/banner.png)

Crawler web experimental, distribuído e observável, projetado para exploração controlada da web com foco em robustez, controle de carga e rastreabilidade.

---

## Overview

O WackyWacky executa crawling recursivo a partir de queries iniciais, aplicando:

- controle de recursão
- rate limit por domínio
- retry automático
- filtragem de idioma
- bloqueio de domínios
- deduplicação por conteúdo

O sistema é multi-worker e coordenado via banco (MySQL).
A telemetria via OpenTelemetry é opcional: no Compose atual ela fica desligada por padrão e a stack de observabilidade está comentada.

---

## Arquitetura

```

Leader (1)
└── controla estado global (DB)

Workers (N)
└── consomem páginas
└── executam crawling com Playwright via Chromium ou Obscura/CDP

```

- Coordenação distribuída via `GET_LOCK` (MySQL)
- Estado global em `settings.system_status`
- Filas implícitas via tabela `pages`

---

## Fluxo

1. Leader inicializa (`STARTING`)
2. Starter gera URLs iniciais
3. Sistema entra em `RUNNING_MINING`
4. Workers:
   - claim de páginas (`TODO`)
   - processamento
   - extração de links
   - inserção de novas páginas
5. Loop até exaustão ou parada

---

## Principais Componentes

### `App`
Orquestrador:

- leader election
- controle de estado
- gerenciamento de threads
- limpeza de páginas travadas

### `Requester`
Core do crawler:

- navegação via Playwright conectado a Chromium ou Obscura/CDP
- extração de conteúdo
- aplicação de regras
- persistência

### `Page` / `Domain`
Modelo de dados:

- controle de status e retry
- limitação por domínio com precisão subsegundo
- deduplicação por hash

### `Starter`
Geração de seeds:

- Google
- DuckDuckGo
- Bing
- Wikipedia

Configurado via banco.

---

## Features

- Crawling recursivo com limites configuráveis
- Pool de workers multi-thread
- Rate limiting por domínio (cooldown real)
- Retry com controle temporal
- Filtro de idioma (`langdetect`)
- Blocklist eficiente (MD5)
- Deduplicação por conteúdo
- Compressão (zstd)
- Observabilidade opcional (traces, metrics, logs)

---

## Configuração

### Ambiente (`.env`)

Definido em:

```

miner/settings/settings.py

```

Principais:

- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`
- `MAX_THREADS`
- `PROCESSING_TIMEOUT_SECONDS`
- `GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS`
- `BROWSER_BACKEND`
- `OBSCURA_CDP_ENDPOINT`
- `OBSCURA_CDP_CONNECT_TIMEOUT_SECONDS`
- `MINER_TELEMETRY_ENABLED`
- `SAVE_HTML`
- `MAX_CHARACTERS_TEXT`

---

### Dinâmica (MySQL → tabela `settings`)

Sem necessidade de restart:

- `init_terms`
- `search_engine`
- `max_recursion`
- `max_retry_attempts`
- `retry_interval_ms`
- `domain_request_interval_ms`
- `system_status`

`domain_request_interval_ms` é respeitado em milissegundos usando `TIMESTAMP(6)` em `domain.last_request_at`.

---

## Execução

### Infraestrutura

```

docker compose up -d

```

Serviços:

- MySQL
- Obscura
- Workers
- phpMyAdmin

O `compose.yml` atual define 11 pares `obscura-N`/`worker-N`, de `1` a `11`.
Cada worker usa `MAX_THREADS=1` e aponta para seu Obscura dedicado via `OBSCURA_CDP_ENDPOINT`.

Serviços de observabilidade estão no arquivo, mas comentados por padrão:

- Grafana
- Prometheus
- Loki
- Jaeger
- OTEL Collector

---

### Setup

```

pip install -r requirements.txt

# somente para uso local com BROWSER_BACKEND=chromium
playwright install chromium

```

---

### Run

```

python -m miner.main

```

Reset do banco:

```

python -m miner.main --reset-db

```

Scripts utilitários:

```

# limpa pages/domain, libera lock de líder e restaura system_status=starting
python reset_db.py

# imprime previews de textos salvos em pages.text
python preview_page_text.py

```

---

## Banco de Dados

Entidades principais:

- `pages` → fila + conteúdo
- `domain` → controle de rate limit
- `blocked_domain` → blacklist
- `settings` → config dinâmica

Detalhes relevantes:

- `domain.last_request_at` usa `TIMESTAMP(6)` para cooldown subsegundo
- `pages.text` e `pages.html` usam `MEDIUMBLOB`
- `pages.text` é salvo comprimido com zstd
- `pages.html` só é salvo quando `SAVE_HTML=True`

Migrações manuais para produção ficam em `sqls/`, incluindo:

```

sqls/alter_pages_content_mediumblob.sql
sqls/alter_domain_cooldown_timestamp6.sql

```

Essas migrações precisam ser aplicadas em bancos já existentes. O `mysql_init/` só afeta bancos criados do zero.

---

## Concorrência

- Threads limitadas por `MAX_THREADS`
- Claim em batch de 20 com `FOR UPDATE SKIP LOCKED`
- Cada batch reserva no máximo uma página por domínio
- Lock distribuído (leader)
- Cache de IDs para reduzir contenção
- Lease de páginas `processing` controlado por `PROCESSING_TIMEOUT_SECONDS`

---

## Observabilidade

OpenTelemetry integrado:

- tracing distribuído
- métricas customizadas
- logs centralizados

Controle via ambiente:

- `MINER_TELEMETRY_ENABLED=true` envia traces, métricas e logs para OTLP
- `MINER_TELEMETRY_ENABLED=false` mantém logs no stdout e desliga o report OpenTelemetry do miner
- A flag não inicia nem para Grafana, Prometheus, Loki, Jaeger ou OTEL Collector

Stack disponível no Compose, comentada por padrão:

- Grafana → dashboards
- Prometheus → métricas
- Loki → logs
- Jaeger → traces

---

## Regras de Crawling

Uma página só é processada se:

- não excedeu retry
- está dentro do limite de recursão
- domínio não está bloqueado
- domínio não está em cooldown

Caso contrário:

- status atualizado
- processamento interrompido

---

## Armazenamento

- Texto sempre salvo
- HTML opcional (`SAVE_HTML`)
- Compressão com zstd
- Deduplicação automática (hash)

---

## Estrutura do Projeto

```

miner/
mysql_init/
sqls/
loki/
prometheus/
otel/
reset_db.py
preview_page_text.py
README.md

```

---

## Limitações

- Sem exportação nativa dos dados
- Balanceamento por domínio ainda é baseado em claim + cooldown, não em fila dedicada
- Sem priorização de URLs

---

## Roadmap

- Export de dados
- Heartbeat para páginas em processamento
- Add queue mechanism to reduce DB workload (Redis?)
- Melhorar deduplicação por conteúdo fazendo uma normalização mais eficiente
- Adicionar referência ao https://dsi.ut-capitole.fr/blacklists/index_en.php

---

## Licença

Veja: LICENSE.md
