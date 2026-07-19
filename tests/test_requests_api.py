import os
import unittest
from unittest.mock import patch

os.environ.setdefault("GOOGLE_CLIENT_ID", "test-client-id")
os.environ.setdefault("GOOGLE_CLIENT_SECRET", "test-client-secret")

from fastapi.testclient import TestClient

from streaming.requests import requests_api


class RequestsApiAuthTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(requests_api.app)
        self.payload = {
            "user_id": 999,
            "blood_group": "O+",
            "city": "Delhi",
            "units_required": 2,
            "latitude": 28.61,
            "longitude": 77.21,
        }

    def test_create_requests_requires_authentication(self):
        with patch("streaming.requests.requests_api.publish_request") as publish_request:
            response = self.client.post("/requests", json=self.payload)

        self.assertEqual(response.status_code, 401)
        publish_request.assert_not_called()

    def test_create_requests_uses_authenticated_user_identity(self):
        requests_api.app.dependency_overrides[requests_api.get_current_user] = lambda: {"id": 42, "email": "user@example.com"}

        with patch("streaming.requests.requests_api.publish_request") as publish_request:
            response = self.client.post("/requests", json=self.payload)

        self.assertEqual(response.status_code, 200)
        publish_request.assert_called_once()
        payload = publish_request.call_args.args[0]
        self.assertEqual(payload["user_id"], 42)

        requests_api.app.dependency_overrides.clear()


if __name__ == "__main__":
    unittest.main()
