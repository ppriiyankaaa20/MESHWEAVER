"""Secure asyncio task mesh primitives for the Week 4 demo."""

import asyncio
import base64
import json
import ssl
import time
import uuid
from pathlib import Path

import cloudpickle

from security import load_private_key, load_public_key, sign_message, verify_signature


def _canonical_message(request):
    return json.dumps(
        {key: request[key] for key in ("task_id", "sender", "payload")},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _encode(value):
    return base64.b64encode(cloudpickle.dumps(value)).decode("ascii")


def _decode(value):
    return cloudpickle.loads(base64.b64decode(value))


def build_server_context(cert_dir, node_name):
    cert_dir = Path(cert_dir)
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cert_dir / "ca.crt")
    context.load_cert_chain(cert_dir / f"{node_name}_tls.crt", cert_dir / f"{node_name}_tls.key")
    return context


def build_client_context(cert_dir, node_name):
    cert_dir = Path(cert_dir)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cert_dir / "ca.crt")
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = False
    context.load_cert_chain(cert_dir / f"{node_name}_tls.crt", cert_dir / f"{node_name}_tls.key")
    return context


class SecureNode:
    """A mutually-authenticated TLS node that accepts only signed task requests."""

    def __init__(self, node_name, host, port, cert_dir="certs", key_dir="certs"):
        self.node_name = node_name
        self.host = host
        self.port = port
        self.cert_dir = Path(cert_dir)
        self.key_dir = Path(key_dir)
        self.private_key = load_private_key(self.key_dir / f"{node_name}_private.pem")
        self.public_keys = {}
        self.server = None
        self.tasks = {}
        self.peers = set()

    async def start(self):
        self.server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            ssl=build_server_context(self.cert_dir, self.node_name),
        )
        return self

    async def stop(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    def register_public_key(self, node_name):
        self.public_keys[node_name] = load_public_key(
            self.key_dir / f"{node_name}_public.pem"
        )

    async def _handle_client(self, reader, writer):
        certificate_name = self._peer_common_name(writer)
        if certificate_name:
            self.peers.add(certificate_name)
        try:
            request = json.loads(await reader.readline())
            response = await self._execute_request(request, certificate_name)
            writer.write((json.dumps(response) + "\n").encode("utf-8"))
            await writer.drain()
        except (OSError, ValueError, json.JSONDecodeError) as error:
            writer.write((json.dumps({"ok": False, "error": str(error)}) + "\n").encode())
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    @staticmethod
    def _peer_common_name(writer):
        certificate = writer.get_extra_info("peercert") or {}
        for attribute in certificate.get("subject", ()):
            for key, value in attribute:
                if key == "commonName":
                    return value
        return None

    async def _execute_request(self, request, certificate_name):
        required = {"task_id", "sender", "payload", "signature"}
        if set(request) != required:
            return {"ok": False, "error": "invalid task envelope"}
        if certificate_name != request["sender"]:
            return {"ok": False, "error": "certificate identity mismatch"}
        public_key = self.public_keys.get(request["sender"])
        signature = base64.b64decode(request["signature"])
        if public_key is None or not verify_signature(public_key, _canonical_message(request), signature):
            return {"ok": False, "error": "invalid signature"}

        task_id = request["task_id"]
        self.tasks[task_id] = {"state": "running", "started": time.time()}
        try:
            function, arguments = _decode(request["payload"])
            result = function(*arguments)
            self.tasks[task_id].update(state="completed", result=result)
            return {"ok": True, "task_id": task_id, "result": _encode(result)}
        except Exception as error:
            self.tasks[task_id].update(state="failed", error=str(error))
            return {"ok": False, "task_id": task_id, "error": str(error)}

    async def serve_forever(self):
        if self.server is None:
            await self.start()
        async with self.server:
            await self.server.serve_forever()


async def submit_task(host, port, sender, function, arguments, cert_dir="certs", key_dir="certs"):
    """Submit one signed task and return the decoded result."""
    private_key = load_private_key(Path(key_dir) / f"{sender}_private.pem")
    request = {
        "task_id": str(uuid.uuid4()),
        "sender": sender,
        "payload": _encode((function, arguments)),
    }
    request["signature"] = base64.b64encode(
        sign_message(private_key, _canonical_message(request))
    ).decode("ascii")
    reader, writer = await asyncio.open_connection(
        host, port, ssl=build_client_context(cert_dir, sender), server_hostname="localhost"
    )
    writer.write((json.dumps(request) + "\n").encode("utf-8"))
    await writer.drain()
    response = json.loads(await reader.readline())
    writer.close()
    await writer.wait_closed()
    if not response.get("ok"):
        raise PermissionError(response.get("error", "task rejected"))
    return _decode(response["result"])


def topology_snapshot(nodes):
    return {
        name: {
            "port": node.port,
            "peers": sorted(node.peers),
            "tasks": {task_id: task["state"] for task_id, task in node.tasks.items()},
        }
        for name, node in nodes.items()
    }