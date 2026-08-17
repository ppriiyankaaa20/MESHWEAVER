import asyncio


class UDPServerProtocol(asyncio.DatagramProtocol):

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        message = data.decode()

        print(f"Received from {addr}: {message}")

        if message == "PING":
            response = "PONG"

            self.transport.sendto(
                response.encode(),
                addr
            )


async def main():

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol(),
        local_addr=("127.0.0.1", 8000)
    )

    print("MeshWeaver Node B started...")
    print("Listening on 127.0.0.1:8000")

    try:
        await asyncio.Future()

    finally:
        transport.close()


asyncio.run(main())