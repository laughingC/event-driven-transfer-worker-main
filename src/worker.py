# Cross-cloud transfer worker: S3 -> GCS via WIF.
# Uses tenacity for retries, writes failed events to a DLQ file.

import hashlib
import json
import logging
import os
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from google.cloud import storage
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

class JsonFormatter(logging.Formatter):
    """Spits out one JSON line per log record."""

    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "correlationId": os.environ.get("CORRELATION_ID", str(uuid.uuid4())),
            "logger": record.name,
        }
        if record.exc_info and record.exc_info[1]:
            log_entry["error"] = str(record.exc_info[1])
        return json.dumps(log_entry)


logger = logging.getLogger("transfer-worker")
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(JsonFormatter())
logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# counters -- would use prometheus_client in a real deploy
_metrics = {
    "events_received": 0,
    "events_processed": 0,
    "events_failed": 0,
    "events_skipped_duplicate": 0,
    "download_bytes": 0,
}


def emit_metrics():
    logger.info(f"metrics: {json.dumps(_metrics)}")


class TransferEvent:

    def __init__(self, event_dict: dict):
        self.raw = event_dict
        self.event_id = event_dict.get("eventId")
        self.schema_version = event_dict.get("schemaVersion", "1.0")
        self.timestamp = event_dict.get("timestamp")
        self.correlation_id = event_dict.get("correlationId", str(uuid.uuid4()))

        src = event_dict.get("source", {})
        dst = event_dict.get("destination", {})

        self.src_provider = src.get("provider")
        self.src_bucket = src.get("bucket")
        self.src_key = src.get("key")
        self.dst_provider = dst.get("provider")
        self.dst_bucket = dst.get("bucket")
        self.dst_key = dst.get("key")

        # derive idempotency key if caller didn't supply one
        key_input = f"{self.src_bucket}/{self.src_key}:{self.dst_bucket}/{self.dst_key}"
        self.idempotency_key = event_dict.get(
            "idempotencyKey",
            hashlib.sha256(key_input.encode()).hexdigest()[:16],
        )

    def validate(self) -> list[str]:
        errors = []
        if not self.event_id:
            errors.append("missing eventId")
        if self.src_provider != "aws":
            errors.append(f"unsupported source provider: {self.src_provider}")
        if self.dst_provider != "gcp":
            errors.append(f"unsupported destination provider: {self.dst_provider}")
        if not self.src_bucket or not self.src_key:
            errors.append("missing source bucket/key")
        if not self.dst_bucket or not self.dst_key:
            errors.append("missing destination bucket/key")
        return errors


def check_idempotent(gcs_client, bucket_name: str, key: str) -> bool:
    """True if the dest blob already exists (skip re-transfer)."""
    try:
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(key)
        return blob.exists()
    except Exception:
        return False


# DLQ -- just a file for now; in prod this maps to an SQS dead-letter queue
import tempfile as _tempfile
DLQ_PATH = os.environ.get("DLQ_PATH", os.path.join(_tempfile.gettempdir(), "dlq.json"))


def send_to_dlq(event_dict: dict, error_msg: str):
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error": error_msg,
        "event": event_dict,
    }
    logger.error(f"DLQ: routing failed event -- {error_msg}")
    _metrics["events_failed"] += 1

    existing = []
    if os.path.exists(DLQ_PATH):
        with open(DLQ_PATH, "r") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []
    existing.append(entry)
    with open(DLQ_PATH, "w") as f:
        json.dump(existing, f, indent=2)


@retry(
    wait=wait_exponential_jitter(initial=2, max=30, jitter=2),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((ClientError, ConnectionError, OSError)),
    reraise=True,
)
def download_from_s3(s3_client, bucket: str, key: str, local_path: str) -> int:
    logger.info(f"Downloading s3://{bucket}/{key}")
    s3_client.download_file(bucket, key, local_path)
    size = os.path.getsize(local_path)
    _metrics["download_bytes"] += size
    return size


@retry(
    wait=wait_exponential_jitter(initial=2, max=30, jitter=2),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((ConnectionError, OSError)),
    reraise=True,
)
def upload_to_gcs(gcs_client, bucket: str, key: str, local_path: str):
    logger.info(f"Uploading gs://{bucket}/{key}")
    gcs_bucket = gcs_client.bucket(bucket)
    blob = gcs_bucket.blob(key)
    blob.upload_from_filename(local_path)
    logger.info(f"Upload complete: gs://{bucket}/{key}")


def process_event(event_dict: dict, s3_client=None, gcs_client=None) -> bool:
    _metrics["events_received"] += 1
    start = time.time()

    # parse + validate
    event = TransferEvent(event_dict)
    errors = event.validate()
    if errors:
        send_to_dlq(event_dict, f"validation failed: {', '.join(errors)}")
        return False

    logger.info(
        f"Processing event={event.event_id} "
        f"idempotencyKey={event.idempotency_key} "
        f"s3://{event.src_bucket}/{event.src_key} -> "
        f"gs://{event.dst_bucket}/{event.dst_key}"
    )

    if s3_client is None:
        s3_client = boto3.client("s3")
    if gcs_client is None:
        gcs_client = storage.Client()

    # skip if dest object already exists (idempotent)
    if check_idempotent(gcs_client, event.dst_bucket, event.dst_key):
        logger.info(
            f"IDEMPOTENT SKIP: gs://{event.dst_bucket}/{event.dst_key} already exists"
        )
        _metrics["events_skipped_duplicate"] += 1
        return True

    with tempfile.NamedTemporaryFile(delete=False, suffix=".transfer") as tmp:
        tmp_path = tmp.name

    try:
        file_size = download_from_s3(s3_client, event.src_bucket, event.src_key, tmp_path)
        upload_to_gcs(gcs_client, event.dst_bucket, event.dst_key, tmp_path)

        duration = time.time() - start
        _metrics["events_processed"] += 1
        logger.info(
            f"Transfer complete: event={event.event_id} "
            f"size={file_size}B duration={duration:.2f}s"
        )
        return True

    except Exception as e:
        send_to_dlq(event_dict, str(e))
        return False

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def build_event_from_env() -> dict:
    return {
        "eventId": os.environ.get("EVENT_ID", str(uuid.uuid4())),
        "schemaVersion": "1.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "correlationId": os.environ.get("CORRELATION_ID", str(uuid.uuid4())),
        "source": {
            "provider": "aws",
            "bucket": os.environ.get("SOURCE_BUCKET", ""),
            "key": os.environ.get("SOURCE_KEY", ""),
        },
        "destination": {
            "provider": "gcp",
            "bucket": os.environ.get("DEST_BUCKET", ""),
            "key": os.environ.get("DEST_KEY", ""),
        },
        "idempotencyKey": os.environ.get("IDEMPOTENCY_KEY", ""),
    }


def poll_sqs(queue_url, sqs_client=None, s3_client=None, gcs_client=None,
             dlq_url=None, max_polls=5):
    """Reads messages from an SQS queue and processes each transfer event.
    Successful transfers get deleted from the queue right away.
    Failed ones get forwarded to the DLQ if a URL is provided,
    otherwise they stay in the queue for the native redrive to handle."""
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    total = 0
    for poll_num in range(max_polls):
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
        )
        messages = resp.get("Messages", [])
        if not messages:
            logger.info(f"Queue empty after {poll_num + 1} poll(s)")
            break

        for msg in messages:
            msg_id = msg["MessageId"]
            receipt = msg["ReceiptHandle"]

            try:
                event_dict = json.loads(msg["Body"])
            except (json.JSONDecodeError, KeyError) as exc:
                logger.error(f"Unparseable SQS message {msg_id}: {exc}")
                if dlq_url:
                    sqs_client.send_message(QueueUrl=dlq_url, MessageBody=msg["Body"])
                    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                continue

            ok = process_event(event_dict, s3_client=s3_client, gcs_client=gcs_client)
            if ok:
                sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                total += 1
                logger.info(f"Deleted SQS message {msg_id}")
            else:
                if dlq_url:
                    sqs_client.send_message(QueueUrl=dlq_url, MessageBody=msg["Body"])
                    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                    logger.warning(f"Routed message {msg_id} to DLQ")
                else:
                    logger.warning(f"Message {msg_id} failed, leaving for redrive")

    return total


def main():
    logger.info("worker starting")

    sqs_url = os.environ.get("SQS_QUEUE_URL")

    if sqs_url:
        # SQS polling mode -- reads from a real queue
        logger.info(f"SQS mode: polling {sqs_url}")
        dlq_url = os.environ.get("SQS_DLQ_URL")
        processed = poll_sqs(sqs_url, dlq_url=dlq_url)
        emit_metrics()
        logger.info(f"done -- {processed} messages processed from SQS")
    else:
        # single-event mode from env vars or a file
        event = build_event_from_env()

        event_file = os.environ.get("EVENT_FILE")
        if event_file and os.path.exists(event_file):
            with open(event_file, "r") as f:
                event = json.load(f)
            logger.info(f"Loaded event from {event_file}")

        success = process_event(event)
        emit_metrics()
        logger.info(f"done ({'ok' if success else 'failed'})")
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
