FROM python:3.12.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      gcc \
      curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium for Playwright in a shared location
RUN python -m playwright install --with-deps chromium

COPY . /app

RUN useradd -m -u 10001 appuser \
    && mkdir -p /ms-playwright \
    && chown -R appuser:appuser /app /ms-playwright

USER appuser

CMD ["python", "-m", "miner.main"]
