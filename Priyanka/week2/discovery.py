import asyncio
import json


class PeerDiscovery(asyncio.DatagramProtocol):

    def __init__(self, node_id, port, known_peers=None):
        self.node_id = node_id
        self.port = port
        self.transport = None

        # Store discovered peers
        self.peers = set(known_peers or [])

    def connection_made(self, transport):

        self.transport = transport

        print(f"Discovery started for {self.node_id}")
        print(f"Listening on port {self.port}")

    def datagram_received(self, data, addr):

        try:
            message = json.loads(data.decode())

            message_type = message.get("type")

            # -----------------------------------------
            # RECEIVE PEER DISCOVERY MESSAGE
            # -----------------------------------------

            if message_type == "DISCOVER":

                peer_id = message["node_id"]
                peer_port = message["port"]

                peer = (addr[0], peer_port)

                if peer not in self.peers:

                    self.peers.add(peer)

                    print(
                        f"[DISCOVERY] New peer found: "
                        f"{peer_id} at {peer}"
                    )

                # Send response
                response = {
                    "type": "PEER_LIST",
                    "node_id": self.node_id,
                    "peers": list(self.peers)
                }

                self.send_message(response, addr)

            # -----------------------------------------
            # RECEIVE PEER LIST
            # -----------------------------------------

            elif message_type == "PEER_LIST":

                for peer in message.get("peers", []):

                    peer = tuple(peer)

                    if peer not in self.peers:
                        self.peers.add(peer)

                        print(
                            f"[DISCOVERY] Added peer: {peer}"
                        )

        except Exception as e:

            print(f"[DISCOVERY ERROR] {e}")

    def send_message(self, message, address):

        data = json.dumps(message).encode()

        self.transport.sendto(data, address)

    def discover_peer(self, address):

        message = {
            "type": "DISCOVER",
            "node_id": self.node_id,
            "port": self.port
        }

        self.send_message(message, address)

    def get_peers(self):

        return list(self.peers)


async def start_discovery(
    node_id,
    port,
    known_peers
):

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