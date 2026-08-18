import asyncio
import uuid

from config import (
    HOST,
    DEFAULT_PORT,
    DEFAULT_PEER_HOST,
    DEFAULT_PEER_PORT,
    PING,
    PONG,
)

from network import MeshNetwork
from protocol import create_ping, create_pong


class MeshNode:

    def __init__(
        self,
        host=HOST,
        port=DEFAULT_PORT,
        peer_host=DEFAULT_PEER_HOST,
        peer_port=DEFAULT_PEER_PORT
    ):
        self.node_id = str(uuid.uuid4())

        self.host = host
        self.port = port

        self.peer_host = peer_host
        self.peer_port = peer_port

        self.network = MeshNetwork(
            message_handler=self.handle_message
        )

    async def start(self):
        await self.network.start(
            self.host,
            self.port
        )

        print(f"Node ID: {self.node_id}")
        print(f"Listening: {self.host}:{self.port}")

    async def handle_message(self, message, address):

        message_type = message.get("type")
        data = message.get("data", {})

        if message_type == PING:

            sender_id = data.get("node_id")

            print(
                f"PING received from "
                f"{sender_id} ({address[0]}:{address[1]})"
            )

            response = create_pong(self.node_id)

            self.network.send(
                response,
                address[0],
                address[1]
            )

        elif message_type == PONG:

            sender_id = data.get("node_id")

            print(
                f"PONG received from "
                f"{sender_id} ({address[0]}:{address[1]})"
            )

        else:

            print(
                f"Unknown message type: {message_type}"
            )

    async def ping_peer(self):

        message = create_ping(
            self.node_id
        )

        self.network.send(
            message,
            self.peer_host,
            self.peer_port
        )

        print(
            f"PING sent to "
            f"{self.peer_host}:{self.peer_port}"
        )

    async def run(self):

        await self.start()

        await asyncio.sleep(1)

        await self.ping_peer()

        try:

            while True:

                await asyncio.sleep(1)

        except asyncio.CancelledError:

            self.stop()

    def stop(self):

        self.network.close()

        print(
            f"Node {self.node_id} stopped."
        )