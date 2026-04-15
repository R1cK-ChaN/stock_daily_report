import json
import unittest
from unittest.mock import Mock, patch

import requests

from src.delivery import feishu


class FeishuDeliveryTests(unittest.TestCase):
    def test_notify_feishu_event_skips_when_disabled(self):
        with patch.dict("os.environ", {"FEISHU_ENABLED": "false"}, clear=False):
            result = feishu.notify_feishu_event("pipeline_failure", "hello")

        self.assertTrue(result["success"])
        self.assertTrue(result["skipped"])
        self.assertEqual(result["provider"], "feishu")

    def test_send_feishu_message_requires_webhook(self):
        with patch.dict("os.environ", {"FEISHU_ENABLED": "true"}, clear=False):
            result = feishu.send_feishu_message("hello", webhook_url="", secret="")

        self.assertFalse(result["success"])
        self.assertFalse(result["skipped"])
        self.assertEqual(result["error"], "No webhook URL configured")

    @patch("src.delivery.common.requests.post")
    def test_send_feishu_message_without_secret_uses_text_payload(self, mock_post):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"code": 0, "msg": "success", "data": {}}
        mock_post.return_value = response

        with patch.dict("os.environ", {"FEISHU_ENABLED": "true"}, clear=False):
            result = feishu.send_feishu_message(
                "hello world",
                webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
                secret="",
            )

        self.assertTrue(result["success"])
        payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(payload["msg_type"], "text")
        self.assertEqual(payload["content"]["text"], "hello world")
        self.assertNotIn("timestamp", payload)
        self.assertNotIn("sign", payload)

    @patch("src.delivery.common.requests.post")
    def test_send_feishu_message_with_secret_adds_signature(self, mock_post):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"code": 0, "msg": "success", "data": {}}
        mock_post.return_value = response

        with patch("src.delivery.feishu.time.time", return_value=1599360473):
            result = feishu.send_feishu_message(
                "signed",
                webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
                secret="demo",
            )

        self.assertTrue(result["success"])
        payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(payload["timestamp"], "1599360473")
        self.assertEqual(payload["sign"], feishu.generate_signature("1599360473", "demo"))

    @patch("src.delivery.common.time.sleep")
    @patch("src.delivery.common.requests.post")
    def test_send_feishu_message_retries_transient_request_errors(self, mock_post, _mock_sleep):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"code": 0, "msg": "success", "data": {}}
        mock_post.side_effect = [requests.Timeout("boom"), response]

        result = feishu.send_feishu_message(
            "retry me",
            webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
            secret="",
            retry_count=2,
        )

        self.assertTrue(result["success"])
        self.assertEqual(mock_post.call_count, 2)
        self.assertEqual(result["attempts"], 2)

    @patch("src.delivery.common.time.sleep")
    @patch("src.delivery.common.requests.post")
    def test_send_feishu_message_does_not_retry_signature_failures(self, mock_post, _mock_sleep):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "code": 19021,
            "msg": "sign match fail or timestamp is not within one hour from current time",
        }
        mock_post.return_value = response

        result = feishu.send_feishu_message(
            "bad sign",
            webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
            secret="bad",
            retry_count=2,
        )

        self.assertFalse(result["success"])
        self.assertEqual(mock_post.call_count, 1)

    @patch("src.delivery.common.requests.post")
    def test_send_feishu_message_truncates_by_payload_byte_limit(self, mock_post):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"code": 0, "msg": "success", "data": {}}
        mock_post.return_value = response

        oversized_text = "中" * 12000
        result = feishu.send_feishu_message(
            oversized_text,
            webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
            secret="",
        )

        self.assertTrue(result["success"])
        payload = mock_post.call_args.kwargs["json"]
        payload_bytes = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        self.assertLessEqual(payload_bytes, feishu.MAX_REQUEST_BYTES)
        self.assertIn("消息已截断", payload["content"]["text"])


if __name__ == "__main__":
    unittest.main()
