import asyncio
import json

from discovery import start_discovery
from gossip import GossipProtocol


class MeshNode(asyncio.DatagramProtocol):

    def __init__(self, node_id, port, known_peers):

        self.node_id = node_id
        self.port = port
        self.known_peers = known_peers

        self.transport = None
        self.discovery = None
        self.gossip = None

    # ------------------------------------------------
    # NODE STARTED
    # ------------------------------------------------

    def connection_made(self, transport):

        self.transport = transport

        print("=" * 50)
        print("MeshWeaver Week 2 Node")
        print(f"Node ID : {self.node_id}")
        print(f"Port    : {self.port}")
        print("=" * 50)

    # ------------------------------------------------
    # RECEIVE MESSAGE
    # ------------------------------------------------

    def datagram_received(self, data, addr):

        try:

            message = json.loads(data.decode())

            message_type = message.get("type")

            # ----------------------------------------
            # GOSSIP MESSAGE
            # ----------------------------------------

            if message_type == "GOSSIP":

                self.gossip.receive_gossip(
                    message,
                    addr
                )

            # ----------------------------------------
            # DISCOVERY MESSAGE
            # ----------------------------------------

            elif message_type in (
                "DISCOVER",
                "PEER_LIST"
            ):

                self.discovery.datagram_received(
                    data,
                    addr
                )

        except Exception as e:

            print(f"[NODE ERROR] {e}")


# ----------------------------------------------------
# START NODE
# ----------------------------------------------------

async def start_node(
    node_id,
    port,
    known_peers
):

    loop = asyncio.get_running_loop()

    # -----------------------------------------------
    # Create UDP socket
    # -----------------------------------------------

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: MeshNode(
            node_id,
            port,
            known_peers
        ),
        local_addr=("127.0.0.1", port)
    )

    # -----------------------------------------------
    # Create discovery object
    # -----------------------------------------------

    discovery_transport, discovery = await start_discovery(
        node_id,
        port + 10000,
        known_peers
    )

    protocol.discovery = discovery

    # -----------------------------------------------
    # Create gossip object
    # -----------------------------------------------

    gossip = GossipProtocol(
        node_id,
        transport,
        discovery
    )

    protocol.gossip = gossip

    # -----------------------------------------------
    # Discover known peers
    # -----------------------------------------------

    await asyncio.sleep(1)

    for peer in known_peers:

        discovery.discover_peer(
            tuple(peer)
        )

    # -----------------------------------------------
    # Start gossip
    # -----------------------------------------------

    asyncio.create_task(
        gossip.gossip()
    )

    # -----------------------------------------------
    # Keep node running
    # -----------------------------------------------

    try:

        while True:

            await asyncio.sleep(10)

            gossip.show_peer_status()

    except asyncio.CancelledError:

        pass

    finally:

        transport.close()
        discovery_transport.close()


# ----------------------------------------------------
# MAIN
# ----------------------------------------------------

if __name__ == "__main__":

    import sys

    if len(sys.argv) < 3:

        print(
            "Usage: py node.py <node_id> <port> [peer_port]"
        )

        print(
            "Example: py node.py A 9001 9002"
        )

        sys.exit(1)

    node_id = sys.argv[1]
    port = int(sys.argv[2])

    known_peers = []

    if len(sys.argv) >= 4:

        peer_port = int(sys.argv[3])

        known_peers.append(
            ("127.0.0.1", peer_port)
        )

    asyncio.run(
        start_node(
            node_id,
            port,
            known_peers
        )
    )