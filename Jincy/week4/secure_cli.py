"""Run a local TLS mesh with a live Rich dashboard."""

import argparse
import asyncio
from pathlib import Path

from dashboard import dashboard_loop
from secure_mesh import SecureNode, submit_task


def add(left, right):
    return left + right


async def run(node_count, host, first_port, cert_dir, key_dir):
    cert_dir = Path(cert_dir)
    key_dir = Path(key_dir)
    nodes = {}
    try:
        for offset in range(node_count):
            name = f"node{first_port + offset}"
            node = SecureNode(name, host, first_port + offset, cert_dir, key_dir)
            for peer_offset in range(node_count):
                if peer_offset != offset:
                    node.peers.add(f"node{first_port + peer_offset}")
            for peer_offset in range(node_count):
                node.register_public_key(f"node{first_port + peer_offset}")
            await node.start()
            nodes[name] = node

        dashboard = asyncio.create_task(dashboard_loop(nodes))
        await asyncio.sleep(1)
        result = await submit_task(
            host,
            first_port,
            f"node{first_port + 1}",
            add,
            (10, 20),
            cert_dir,
            key_dir,
        )
        print(f"Signed task result: {result}")
        await dashboard
    finally:
        for node in nodes.values():
            await node.stop()


def main():
    parser = argparse.ArgumentParser(description="Run the MeshWeaver secure CLI dashboard")
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--first-port", type=int, default=8001)
    parser.add_argument("--cert-dir", default="certs")
    parser.add_argument("--key-dir", default="certs")
    args = parser.parse_args()
    try:
        asyncio.run(run(args.nodes, args.host, args.first_port, args.cert_dir, args.key_dir))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()