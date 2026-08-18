import argparse
import asyncio

from node import MeshNode


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="MeshWeaver P2P Async Task Broker"
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1"
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8000
    )

    parser.add_argument(
        "--peer-host",
        default="127.0.0.1"
    )

    parser.add_argument(
        "--peer-port",
        type=int,
        default=8001
    )

    return parser.parse_args()


async def main():
    args = parse_arguments()

    node = MeshNode(
        host=args.host,
        port=args.port,
        peer_host=args.peer_host,
        peer_port=args.peer_port
    )

    try:
        await node.run()

    except KeyboardInterrupt:
        node.stop()


if __name__ == "__main__":
    asyncio.run(main())