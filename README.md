# WackyWacky

WackyWacky is a lightweight experimental **web crawler** focused on controlled web exploration.

The crawler collects pages, extracts links, and recursively visits discovered URLs while applying **domain filtering**, **language detection**, and **rate-limiting**.

The project also includes a complete **observability stack** for monitoring crawler behavior and performance.

---

# Features

- Recursive web crawling
- Domain blocklist filtering
- Language filtering
- Domain rate-limiting
- Retry and recursion limits
- Multi-threaded workers
- Observability with metrics, logs, and traces
- Multi-query crawler starter
- Save compressed text and html data

* By default, only the texts are saved to reduce the amount of generated data. Enable html saving in the settings.

---

# Multi Query Starter

The crawler begins by generating a set of initial queries.

By default, queries are executed against **Wikipedia**:

```

miner/starter/wikipedia.py

```

The search terms used to build the queries are stored in:

```

mysql_init/editable.sql

```

These initial queries seed the crawler with URLs that will be recursively explored.

---

# Observability Stack

The crawler is fully instrumented using **OpenTelemetry**.

Included services:

- Grafana
- Jaeger
- OpenTelemetry Collector
- Loki
- Prometheus

These tools provide:

- distributed tracing
- metrics collection
- centralized logging
- monitoring dashboards

---

# Grafana

Local access:

```

[http://localhost:3000](http://localhost:3000)

```

Default credentials:

```

admin / admin

```

---

# Domain Block List

The crawler uses a domain blacklist to avoid crawling unwanted or unsafe websites.

Default source:

https://dsi.ut-capitole.fr/blacklists/index_en.php

Total entries:

```

5,006,011 domains

```

Because of the size of this dataset, the **first MySQL startup may take some time** while the blocklist is loaded.

---

# Language Filter

Page language is detected using the Python library:

```

langdetect

```

Only pages matching the allowed languages are processed by the crawler.

To change the accepted languages, edit:

```

miner/settings/settings.py

```

Example:

```

Settings.LANGUAGE_TARGETS

```

---

# Requirements

Main runtime requirements:

- Python **3.12**
- Playwright

Local infrastructure requirements:

- Docker
- Docker Compose

Python dependencies are listed in:

```

requirements.txt

```

---

# Project Structure

```

.
├── compose.yml
├── Dockerfile
├── miner/              # crawler implementation
├── mysql_init/         # database schema and initial data
├── loki/
├── prometheus/
├── otel/
├── requirements.txt
└── README.md

```

---

# Running the Stack

Start the infrastructure services:

```

docker compose up -d

```

This launches:

- MySQL
- Grafana
- Loki
- Prometheus
- Jaeger
- OpenTelemetry Collector

Note:

MySQL initialization may take a few minutes due to the **5M domain blocklist**.

---

# Install Python Dependencies

```

pip install -r requirements.txt

```

Install Playwright browser:

```

playwright install chromium

```

---

# Configuration

Configuration is handled through:

- `.env` environment variables
- the `settings` table in MySQL

This allows runtime configuration without rebuilding the crawler.

---

# Running the Crawler

```

python -m miner.main

```

Reset the database:

```

python -m miner.main --reset-db

```

---

# Data Storage

All collected data is stored in **MySQL**, including:

- crawled pages
- extracted text and HTML
- discovered links
- domain metadata

---

# TODO

- Export crawled data from MySQL
- Finish worker Docker image
- Workers with page in `processing` status should peridically refresh the page.updated_at to prevent false release page

---

# License

See:

```

LICENSE.md

```
