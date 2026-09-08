import asyncio
import json


class PeerDiscovery(asyncio.DatagramProtocol):

    def __init__(self, node_id, port, known_peers=None):
        self.node_id = node_id
        self.port = port
        self.transport = None

        # Store peers as:
        # {("127.0.0.1", 9002), ("127.0.0.1", 9003)}
        self.peers = set(known_peers or [])

    def connection_made(self, transport):
        self.transport = transport

        print(
            f"[DISCOVERY] Node {self.node_id} discovery started "
            f"on port {self.port}"
        )

    def datagram_received(self, data, addr):

        try:
            message = json.loads(data.decode())
            message_type = message.get("type")

            # -------------------------------------------------
            # DISCOVER
            # -------------------------------------------------
            if message_type == "DISCOVER":

                peer_id = message.get("node_id")
                peer_port = message.get("port")

                peer = (addr[0], peer_port)

                if peer_id != self.node_id:
                    if peer not in self.peers:
                        self.peers.add(peer)

                        print(
                            f"[DISCOVERY] Node {self.node_id} "
                            f"found Node {peer_id} at {peer}"
                        )

                # Send our peer list back
                response = {
                    "type": "PEER_LIST",
                    "node_id": self.node_id,
                    "port": self.port,
                    "peers": list(self.peers)
                }

                self.send_message(response, addr)

            # -------------------------------------------------
            # PEER LIST
            # -------------------------------------------------
            elif message_type == "PEER_LIST":

                sender_id = message.get("node_id")

                print(
                    f"[DISCOVERY] Node {self.node_id} "
                    f"received peer list from Node {sender_id}"
                )

                for peer in message.get("peers", []):

                    peer = tuple(peer)

                    if peer[0] == "127.0.0.1" and peer[1] != self.port:
                        if peer not in self.peers:

                            self.peers.add(peer)

                            print(
                                f"[DISCOVERY] Node {self.node_id} "
                                f"added peer {peer}"
                            )

        except Exception as e:
            print(f"[DISCOVERY ERROR] {e}")

    def send_message(self, message, address):

        if self.transport is None:
            return

        try:
            data = json.dumps(message).encode()
            self.transport.sendto(data, address)

        except Exception as e:
            print(
                f"[DISCOVERY ERROR] Could not send to {address}: {e}"
            )

    def discover_peer(self, address):

        message = {
            "type": "DISCOVER",
            "node_id": self.node_id,
            "port": self.port
        }

        print(
            f"[DISCOVERY] Node {self.node_id} "
            f"searching for peer at {address}"
        )

        self.send_message(message, address)

    def get_peers(self):

        return list(self.peers)


async def start_discovery(node_id, port, known_peers):

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: PeerDiscovery(
            node_id,
            port,
            known_peers
        ),
        local_addr=("127.0.0.1", port)
    )

    return transport, protocol