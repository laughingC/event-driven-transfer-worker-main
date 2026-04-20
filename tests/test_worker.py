# Tests for the transfer worker. Mocked cloud calls so no creds needed.

import json
import os
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

from src.worker import (
    TransferEvent,
    check_idempotent,
    poll_sqs,
    process_event,
    send_to_dlq,
    _metrics,
)


def _valid_event(**overrides) -> dict:
    base = {
        "eventId": "evt-001",
        "schemaVersion": "1.0",
        "timestamp": "2026-04-20T00:00:00Z",
        "correlationId": "corr-001",
        "source": {"provider": "aws", "bucket": "src-bucket", "key": "data/file.csv"},
        "destination": {"provider": "gcp", "bucket": "dst-bucket", "key": "raw/file.csv"},
        "idempotencyKey": "idem-001",
    }
    base.update(overrides)
    return base


class TestEventValidation(unittest.TestCase):

    def test_valid_event_passes(self):
        event = TransferEvent(_valid_event())
        self.assertEqual(event.validate(), [])

    def test_missing_event_id_fails(self):
        event = TransferEvent(_valid_event(eventId=None))
        errors = event.validate()
        self.assertIn("missing eventId", errors)

    def test_wrong_source_provider_fails(self):
        data = _valid_event()
        data["source"]["provider"] = "azure"
        event = TransferEvent(data)
        errors = event.validate()
        self.assertTrue(any("unsupported source" in e for e in errors))

    def test_wrong_dest_provider_fails(self):
        data = _valid_event()
        data["destination"]["provider"] = "aws"
        event = TransferEvent(data)
        errors = event.validate()
        self.assertTrue(any("unsupported destination" in e for e in errors))

    def test_missing_source_key_fails(self):
        data = _valid_event()
        data["source"]["key"] = ""
        event = TransferEvent(data)
        errors = event.validate()
        self.assertTrue(any("missing source" in e for e in errors))

    def test_idempotency_key_auto_generated(self):
        data = _valid_event()
        del data["idempotencyKey"]
        event = TransferEvent(data)
        self.assertIsNotNone(event.idempotency_key)
        self.assertTrue(len(event.idempotency_key) > 0)


class TestIdempotency(unittest.TestCase):

    def test_new_object_returns_false(self):
        gcs = MagicMock()
        blob = MagicMock()
        blob.exists.return_value = False
        gcs.bucket.return_value.blob.return_value = blob
        self.assertFalse(check_idempotent(gcs, "bucket", "key"))

    def test_existing_object_returns_true(self):
        gcs = MagicMock()
        blob = MagicMock()
        blob.exists.return_value = True
        gcs.bucket.return_value.blob.return_value = blob
        self.assertTrue(check_idempotent(gcs, "bucket", "key"))

    def test_gcs_error_returns_false(self):
        """If GCS blows up, assume the file isn't there yet."""
        gcs = MagicMock()
        gcs.bucket.side_effect = Exception("auth error")
        self.assertFalse(check_idempotent(gcs, "bucket", "key"))


class TestDLQ(unittest.TestCase):

    def test_invalid_event_goes_to_dlq(self):
        bad_event = {"eventId": None, "source": {}, "destination": {}}
        s3 = MagicMock()
        gcs = MagicMock()
        import tempfile, src.worker as w
        dlq_path = os.path.join(tempfile.gettempdir(), "test_dlq_invalid.json")
        original = w.DLQ_PATH
        w.DLQ_PATH = dlq_path
        try:
            result = process_event(bad_event, s3_client=s3, gcs_client=gcs)
            self.assertFalse(result)
        finally:
            w.DLQ_PATH = original
            if os.path.exists(dlq_path):
                os.remove(dlq_path)

    def test_dlq_file_written(self):
        import tempfile
        dlq_path = os.path.join(tempfile.gettempdir(), "test_dlq.json")
        if os.path.exists(dlq_path):
            os.remove(dlq_path)

        with patch.dict(os.environ, {"DLQ_PATH": dlq_path}):
            # Reimport to pick up new DLQ_PATH
            import importlib
            import src.worker as w
            original_path = w.DLQ_PATH
            w.DLQ_PATH = dlq_path
            try:
                send_to_dlq({"eventId": "bad"}, "test error")
                self.assertTrue(os.path.exists(dlq_path))
                with open(dlq_path) as f:
                    entries = json.load(f)
                self.assertEqual(len(entries), 1)
                self.assertEqual(entries[0]["error"], "test error")
            finally:
                w.DLQ_PATH = original_path
                if os.path.exists(dlq_path):
                    os.remove(dlq_path)


class TestHappyPath(unittest.TestCase):

    @patch("src.worker.upload_to_gcs")
    @patch("src.worker.download_from_s3", return_value=1024)
    @patch("src.worker.check_idempotent", return_value=False)
    def test_successful_transfer(self, mock_idem, mock_dl, mock_ul):
        s3 = MagicMock()
        gcs = MagicMock()
        result = process_event(_valid_event(), s3_client=s3, gcs_client=gcs)
        self.assertTrue(result)
        mock_dl.assert_called_once()
        mock_ul.assert_called_once()

    @patch("src.worker.check_idempotent", return_value=True)
    def test_duplicate_skipped(self, mock_idem):
        """Run the same event twice; second time should skip the download."""
        s3 = MagicMock()
        gcs = MagicMock()
        result = process_event(_valid_event(), s3_client=s3, gcs_client=gcs)
        self.assertTrue(result)
        s3.download_file.assert_not_called()


class TestSQSPolling(unittest.TestCase):

    @patch("src.worker.process_event", return_value=True)
    def test_poll_deletes_successful_message(self, mock_process):
        sqs = MagicMock()
        sqs.receive_message.side_effect = [
            {"Messages": [{"MessageId": "m1", "ReceiptHandle": "r1",
                           "Body": json.dumps(_valid_event())}]},
            {"Messages": []},
        ]
        count = poll_sqs("https://sqs.example/q", sqs_client=sqs)
        self.assertEqual(count, 1)
        sqs.delete_message.assert_called_once()

    @patch("src.worker.process_event", return_value=False)
    def test_failed_message_routed_to_dlq(self, mock_process):
        sqs = MagicMock()
        sqs.receive_message.side_effect = [
            {"Messages": [{"MessageId": "m1", "ReceiptHandle": "r1",
                           "Body": json.dumps(_valid_event())}]},
            {"Messages": []},
        ]
        count = poll_sqs("https://sqs.example/q", sqs_client=sqs,
                         dlq_url="https://sqs.example/dlq")
        self.assertEqual(count, 0)
        # failed message should be sent to DLQ and deleted from source
        sqs.send_message.assert_called_once()
        sqs.delete_message.assert_called_once()

    def test_bad_json_not_counted(self):
        sqs = MagicMock()
        sqs.receive_message.side_effect = [
            {"Messages": [{"MessageId": "m1", "ReceiptHandle": "r1",
                           "Body": "not valid json"}]},
            {"Messages": []},
        ]
        count = poll_sqs("https://sqs.example/q", sqs_client=sqs)
        self.assertEqual(count, 0)
        sqs.delete_message.assert_not_called()


if __name__ == "__main__":
    unittest.main()
