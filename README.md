
# WackyWacky

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

O sistema é multi-worker, coordenado via banco (MySQL), e totalmente instrumentado com OpenTelemetry.

---

## Arquitetura

```

Leader (1)
└── controla estado global (DB)

Workers (N)
└── consomem páginas
└── executam crawling com Playwright

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

- navegação via Playwright
- extração de conteúdo
- aplicação de regras
- persistência

### `Page` / `Domain`
Modelo de dados:

- controle de status e retry
- limitação por domínio
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
- Observabilidade completa (traces, metrics, logs)

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
- `SAVE_HTML`

---

### Dinâmica (MySQL → tabela `settings`)

Sem necessidade de restart:

- `init_terms`
- `search_engine`
- `max_recursion`
- `max_retry_attempts`
- `domain_request_interval_ms`
- `system_status`

---

## Execução

### Infraestrutura

```

docker compose up -d

```

Serviços:

- MySQL
- Grafana
- Prometheus
- Loki
- Jaeger
- OTEL Collector

---

### Setup

```

pip install -r requirements.txt
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

---

## Banco de Dados

Entidades principais:

- `pages` → fila + conteúdo
- `domain` → controle de rate limit
- `blocked_domain` → blacklist
- `settings` → config dinâmica

---

## Concorrência

- Threads limitadas por `MAX_THREADS`
- Claim com `FOR UPDATE SKIP LOCKED`
- Lock distribuído (leader)
- Cache de IDs para reduzir contenção

---

## Observabilidade

OpenTelemetry integrado:

- tracing distribuído
- métricas customizadas
- logs centralizados

Stack:

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
loki/
prometheus/
otel/
README.md

```

---

## Limitações

- Sem exportação nativa dos dados
- Balanceamento por domínio simples
- Sem priorização de URLs

---

## Roadmap

- Export de dados
- Melhor scheduler
- Backoff adaptativo por domínio
- Heartbeat para páginas em processamento

---

## Licença

Veja: LICENSE.md
