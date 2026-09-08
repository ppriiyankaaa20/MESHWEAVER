import asyncio
import base64
import unittest
from pathlib import Path

from secure_mesh import SecureNode, _canonical_message, _encode, submit_task
from security import load_private_key, sign_message


ROOT = Path(__file__).parent
CERTS = ROOT / "certs"


def add(left, right):
    return left + right


class SecureMeshTests(unittest.IsolatedAsyncioTestCase):
    async def test_mutual_tls_and_signed_task(self):
        node = SecureNode("node8001", "127.0.0.1", 0, CERTS, CERTS)
        node.register_public_key("node8002")
        await node.start()
        port = node.server.sockets[0].getsockname()[1]
        try:
            result = await submit_task("127.0.0.1", port, "node8002", add, (10, 20), CERTS, CERTS)
            self.assertEqual(result, 30)
            self.assertEqual(len(node.tasks), 1)
            self.assertEqual(next(iter(node.tasks.values()))["state"], "completed")
        finally:
            await node.stop()

    async def test_tampered_signed_task_is_rejected(self):
        node = SecureNode("node8001", "127.0.0.1", 0, CERTS, CERTS)
        node.register_public_key("node8002")
        private_key = load_private_key(CERTS / "node8002_private.pem")
        request = {"task_id": "tampered", "sender": "node8002", "payload": _encode((add, (1, 2)))}
        request["signature"] = base64.b64encode(sign_message(private_key, _canonical_message(request))).decode()
        request["payload"] = _encode((add, (99, 1)))
        response = await node._execute_request(request, "node8002")
        self.assertEqual(response, {"ok": False, "error": "invalid signature"})