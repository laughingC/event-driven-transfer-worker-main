# Cross-Cloud Transfer Worker

Python worker that moves files from AWS S3 to GCP GCS. Auth is done through Workload Identity Federation -- no static keys anywhere.

This runs against real free-tier AWS and GCP accounts via GitHub Actions. Each workflow run seeds a timestamped file in S3, transfers it to GCS, then runs again to prove the idempotency check works.

## How to run

```bash
pip install -r requirements.txt

# tests (no cloud creds needed)
python -m pytest tests/ -v

# single-event transfer (env vars)
export SOURCE_BUCKET=your-s3-bucket  SOURCE_KEY=incoming/sample.json
export DEST_BUCKET=your-gcs-bucket   DEST_KEY=processed/sample.json
python src/worker.py

# SQS queue mode (polls a real queue, routes failures to DLQ)
export SQS_QUEUE_URL=https://sqs.ap-southeast-2.amazonaws.com/123456789/transfer-worker-queue
export SQS_DLQ_URL=https://sqs.ap-southeast-2.amazonaws.com/123456789/transfer-worker-dlq
python src/worker.py
```

## Docker

```bash
docker build -t transfer-worker .   # runs tests during the build
docker run -e SOURCE_BUCKET=... -e DEST_BUCKET=... transfer-worker
```

## CI pipeline

Three jobs chained together:

1. **test-and-scan** -- pytest + Bandit SAST. No cloud creds needed.
2. **docker-build** -- builds image, runs Trivy for container vulns.
3. **transfer** -- authenticates to both clouds via OIDC/WIF, then:
   - Seeds a test file in S3, transfers it to GCS via env vars, re-runs to prove idempotency.
   - Creates real SQS queues (main + DLQ with redrive policy), sends 3 valid + 1 invalid message, polls via `SQS_QUEUE_URL`. Valid transfers land in GCS; the bad message gets routed to the DLQ.
   - Prints SQS queue stats so you can cross-check in the AWS Console.

## Example event

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

## Auth approach

**AWS**: GitHub Actions OIDC token is exchanged for a short-lived STS session. The IAM role allows `s3:GetObject` + `s3:PutObject` on the bucket, plus SQS actions (`sqs:CreateQueue`, `sqs:SendMessage`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes`, `sqs:SetQueueAttributes`, `sqs:PurgeQueue`) scoped to `transfer-worker-*` queues. No access keys anywhere.

**GCP**: Same OIDC token goes through a Workload Identity Pool. The bound service account only has `storage.objectCreator` on one bucket -- it literally cannot read or delete anything. Tokens expire in an hour.

## SLOs

- Transfer success rate: 99.5%
- p95 latency: under 30 seconds

## Decisions worth calling out

- **WIF instead of static keys** -- credentials rotate automatically and expire in 1h.
- **Exponential backoff with jitter** -- avoids thundering herd when a transient failure hits.
- **Idempotency via GCS blob.exists()** -- simple and works. Could swap in Redis or DynamoDB if we needed it at scale.
- **File-based DLQ + real SQS DLQ** -- file DLQ is a local log; in CI the workflow creates actual SQS queues with a redrive policy. The worker also does client-side routing (failed messages get sent to the DLQ queue immediately) so portal evidence is instant.
- **Bandit + Trivy in CI** -- covers both source-level (hardcoded secrets, eval, SQL injection) and container-level (CVEs) scanning.

## Layout

```
src/
  worker.py          core transfer logic, retry, idempotency, SQS polling, DLQ routing
tests/
  test_worker.py     16 tests covering validation, dedup, DLQ, happy path, SQS polling
.github/workflows/
  worker.yml         3-job CI pipeline with real SQS queues
Dockerfile           multi-stage, non-root, healthcheck
event_schema.json    event contract + compatibility notes
ops_profile.json     logging/metrics/retry/identity config
```