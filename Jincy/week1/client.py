import asyncio


class UDPClientProtocol(asyncio.DatagramProtocol):

    def connection_made(self, transport):
        self.transport = transport

        message = "PING"

        print("Sending:", message)

        self.transport.sendto(
            message.encode(),
            ("127.0.0.1", 8000)
        )

    def datagram_received(self, data, addr):
        message = data.decode()

        print("Received:", message)

        self.transport.close()


async def main():

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(),
        local_addr=("127.0.0.1", 0)
    )

    await asyncio.sleep(2)

    transport.close()


asyncio.run(main())