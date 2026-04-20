# Cross-Cloud Transfer Worker

Picks up files from S3 and drops them into GCS. Built this for the Megt case study to show a working cross-cloud pipeline with proper auth (no static keys, everything goes through OIDC/WIF).

The whole thing runs on GitHub Actions against my free-tier AWS and GCP accounts. Each run creates timestamped files so you can see the history building up in both consoles.

## Running it

```bash
pip install -r requirements.txt
python -m pytest tests/ -v          # 16 tests, all mocked, no creds needed

# one-off transfer
export SOURCE_BUCKET=my-bucket  SOURCE_KEY=incoming/file.json
export DEST_BUCKET=my-gcs-bucket DEST_KEY=processed/file.json
python src/worker.py

# or point it at an SQS queue and it'll poll + process + delete
export SQS_QUEUE_URL=https://sqs.ap-southeast-2.amazonaws.com/...
export SQS_DLQ_URL=https://sqs.ap-southeast-2.amazonaws.com/...
python src/worker.py
```

## Docker

```bash
docker build -t transfer-worker .   # tests run during the build
docker run -e SOURCE_BUCKET=... -e DEST_BUCKET=... transfer-worker
```

Image gets pushed to ECR on every merge to main. ECR has scan-on-push enabled so AWS runs its own vulnerability check on top of the Trivy scan we do in CI.

## What the CI pipeline does

Three jobs, each depends on the previous:

**test-and-scan** — runs pytest and Bandit (SAST). Catches bugs and things like hardcoded secrets or unsafe eval calls.

**docker-build** — builds the image, scans it with Trivy for CVEs, pushes to ECR.

**transfer** — the interesting one. Logs into AWS via OIDC and GCP via WIF, then:
- Puts a test file in S3, transfers it to GCS, runs again to show the idempotency skip
- Spins up two SQS queues (main + DLQ with a redrive policy), sends 4 messages (3 good, 1 bad), polls them through the worker. Good ones get transferred and deleted, the bad one lands in the DLQ.
- Dumps queue stats at the end so I can cross-check against the AWS console

## Event format

```json
{
  "schemaVersion": "1.0",
  "eventId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2026-04-20T02:30:00Z",
  "correlationId": "run-12345",
  "idempotencyKey": "sha256-abc123def456",
  "source": {
    "provider": "aws",
    "bucket": "vendor-data-exports",
    "key": "incoming/2026-04-20/orders.csv"
  },
  "destination": {
    "provider": "gcp",
    "bucket": "megt-data-lake-raw",
    "key": "processed/2026-04-20/orders.csv"
  }
}
```

## How auth works

**AWS** — GitHub's OIDC token gets exchanged for a short-lived STS session. The IAM role is scoped to one S3 bucket and queues named `transfer-worker-*`. No access keys stored anywhere.

**GCP** — Same OIDC token goes through Workload Identity Federation. The service account can write to one GCS bucket and that's it. Tokens expire in an hour.

## Things I'd do differently at scale

- Swap the file-based DLQ for a proper SQS dead-letter queue (already demoed in CI but the worker also writes to a local file as a fallback)
- Replace `blob.exists()` idempotency with DynamoDB or Redis if throughput gets high
- Add Prometheus metrics instead of the JSON counters
- Pin the base Docker image to a digest instead of just `3.12-slim`

## Why I made certain choices

**WIF over static keys** — tokens rotate automatically, expire in 1h, and I don't have to worry about key leaks.

**Exponential backoff with jitter** — if S3 or GCS has a blip, retries spread out instead of all hitting at once.

**Two layers of scanning** — Bandit catches Python-level stuff (eval, SQL injection patterns), Trivy catches OS/package CVEs in the container. ECR's scan-on-push adds a third layer on the AWS side.

**SQS redrive policy** — after 2 failed receives, SQS automatically moves the message to the DLQ. The worker also does client-side routing for validation failures so the evidence shows up immediately.

## Files

```
src/worker.py           transfer logic, retry, idempotency, SQS polling
tests/test_worker.py    16 tests (validation, dedup, DLQ, SQS, happy path)
.github/workflows/      CI pipeline — test, build, scan, deploy, transfer
Dockerfile              multi-stage build, non-root user, healthcheck
event_schema.json       event contract
ops_profile.json        operational config (logging, retry, SLOs, identity)
```