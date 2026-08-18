import asyncio

from config import HOST, DEFAULT_PORT, BUFFER_SIZE
from protocol import encode_message, decode_message


class MeshNetwork(asyncio.DatagramProtocol):

    def __init__(self, message_handler=None):
        self.transport = None
        self.message_handler = message_handler
        self.ready = asyncio.Event()

    def connection_made(self, transport):
        self.transport = transport
        self.ready.set()

    def datagram_received(self, data, address):
        try:
            message = decode_message(data)

            if self.message_handler:
                result = self.message_handler(message, address)

                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)

        except Exception as error:
            print(f"Message error: {error}")

    def error_received(self, error):
        print(f"Network error: {error}")

    def connection_lost(self, exception):
        self.transport = None

    async def start(self, host=HOST, port=DEFAULT_PORT):
        loop = asyncio.get_running_loop()

        transport, _ = await loop.create_datagram_endpoint(
            lambda: self,
            local_addr=(host, port)
        )

        self.transport = transport

        await self.ready.wait()

        print(f"Node started on {host}:{port}")

    def send(self, message, host, port):
        if self.transport is None:
            raise RuntimeError("Network is not started")

        data = encode_message(message)

        self.transport.sendto(
            data,
            (host, port)
        )

    async def send_message(self, message, host, port):
        self.send(message, host, port)

    def close(self):
        if self.transport:
            self.transport.close()
            self.transport = None
            