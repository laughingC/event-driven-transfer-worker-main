FROM python:3.12-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
COPY tests/ ./tests/
RUN python -m pytest tests/ -v --tb=short

FROM python:3.12-slim
WORKDIR /app

RUN addgroup --system worker && adduser --system --group worker

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /app/src/ ./src/
RUN chown -R worker:worker /app

USER worker

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import importlib; importlib.import_module('src.worker'); print('healthy')"

ENTRYPOINT ["python", "-m", "src.worker"]
