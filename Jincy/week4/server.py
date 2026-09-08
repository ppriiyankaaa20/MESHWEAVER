import asyncio

from secure_mesh import SecureNode


async def main():
    node = SecureNode("node8001", "127.0.0.1", 8000)
    node.register_public_key("node8002")
    await node.start()
    print("MeshWeaver secure node listening on TLS 127.0.0.1:8000")
    try:
        await node.serve_forever()
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())