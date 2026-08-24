import asyncio
import hashlib
import socket


# ==========================================
# Node ID
# ==========================================

def generate_node_id(hostname, port):

    identity = f"{hostname}:{port}"

    return hashlib.sha1(
        identity.encode()
    ).hexdigest()


# ==========================================
# DHT Node
# ==========================================

class DHTNode(asyncio.DatagramProtocol):

    def __init__(self, port):

        self.port = port
        self.host = "127.0.0.1"

        self.hostname = socket.gethostname()

        self.node_id = generate_node_id(
            self.hostname,
            self.port
        )

        # Known peers
        self.peers = {}

        self.transport = None


    # ======================================
    # UDP connection started
    # ======================================

    def connection_made(self, transport):

        self.transport = transport

        print("================================")
        print("       MeshWeaver DHT Node")
        print("================================")

        print("Hostname :", self.hostname)
        print("IP       :", self.host)
        print("Port     :", self.port)
        print("Node ID  :", self.node_id)

        print("--------------------------------")
        print("Known Peers:", self.peers)
        print("Node is listening...")


    # ======================================
    # Receive UDP messages
    # ======================================

    def datagram_received(self, data, addr):

        message = data.decode()

        print(f"\nReceived from {addr}: {message}")

        # ------------------------------
        # PING
        # ------------------------------

        if message == "PING":

            self.transport.sendto(
                b"PONG",
                addr
            )

            print("Sent: PONG")


        # ------------------------------
        # JOIN
        # ------------------------------

        elif message.startswith("JOIN|"):

            parts = message.split("|")

            joining_node_id = parts[1]
            joining_port = int(parts[2])

            print("Join request received")
            print("Joining Node ID:", joining_node_id)
            print("Joining Node Port:", joining_port)

            # Add joining node to peer table
            self.peers[joining_node_id] = (
                addr[0],
                joining_port
            )

            print("Updated Peer Table:")
            print(self.peers)

            # Send confirmation
            response = f"JOINED|{self.node_id}|{self.port}"

            self.transport.sendto(
                response.encode(),
                addr
            )


        # ------------------------------
        # PEERS request
        # ------------------------------

        elif message == "GET_PEERS":

            print("Peer list requested")

            response = "PEERS"

            for peer_id, peer_address in self.peers.items():

                response += (
                    f"|{peer_id},"
                    f"{peer_address[0]},"
                    f"{peer_address[1]}"
                )

            self.transport.sendto(
                response.encode(),
                addr
            )

            print("Sent peer list")


# ==========================================
# Start Node
# ==========================================

async def start_node(port):

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: DHTNode(port),
        local_addr=("127.0.0.1", port)
    )

    try:

        await asyncio.Future()

    finally:

        transport.close()


# ==========================================
# Main
# ==========================================

if __name__ == "__main__":

    import sys

    if len(sys.argv) != 2:

        print("Usage:")
        print("python dht_node.py <port>")

        print("\nExample:")
        print("python dht_node.py 8001")

        sys.exit(1)

    port = int(sys.argv[1])

    asyncio.run(
        start_node(port)
    )