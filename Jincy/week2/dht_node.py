import asyncio
import hashlib
import socket
import sys
import psutil


# ==========================================
# Node ID
# ==========================================

def generate_node_id(hostname, port):

    identity = f"{hostname}:{port}"

    return hashlib.sha1(
        identity.encode()
    ).hexdigest()


# ==========================================
# Kademlia XOR Distance
# ==========================================

def xor_distance(node_id_1, node_id_2):

    return int(node_id_1, 16) ^ int(node_id_2, 16)


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
        # node_id -> (ip, port)
        self.peers = {}

        # Store latest CPU/RAM statistics
        # node_id -> {"ip": ..., "port": ..., "cpu": ..., "ram": ...}
        self.peer_stats = {}

        self.transport = None

        # Background gossip task
        self.gossip_task = None


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

        # Start background gossip task
        self.gossip_task = asyncio.create_task(
            self.gossip_loop()
        )


    # ======================================
    # Receive UDP messages
    # ======================================

    def datagram_received(self, data, addr):

        try:
            message = data.decode()

        except UnicodeDecodeError:
            print("Received invalid data")
            return

        print(f"\nReceived from {addr}: {message}")


        # ==================================
        # PING
        # ==================================

        if message == "PING":

            self.transport.sendto(
                b"PONG",
                addr
            )

            print("Sent: PONG")


        # ==================================
        # PONG
        # ==================================

        elif message == "PONG":

            print("Peer is alive")


        # ==================================
        # JOIN
        # ==================================

        elif message.startswith("JOIN|"):

            parts = message.split("|")

            if len(parts) != 3:
                print("Invalid JOIN message")
                return

            joining_node_id = parts[1]

            try:
                joining_port = int(parts[2])

            except ValueError:
                print("Invalid joining port")
                return


            print("Join request received")
            print("Joining Node ID:", joining_node_id)
            print("Joining Node Port:", joining_port)


            # Add joining node to peer table
            self.peers[joining_node_id] = (
                addr[0],
                joining_port
            )


            print("Updated Peer Table:")

            self.print_peers()


            # Send confirmation
            response = (
                f"JOINED|{self.node_id}|{self.port}"
            )

            self.transport.sendto(
                response.encode(),
                addr
            )

            print("Sent:", response)


            # Send current peer list to joining node
            self.send_peer_list(addr)


        # ==================================
        # JOINED
        # ==================================

        elif message.startswith("JOINED|"):

            parts = message.split("|")

            if len(parts) != 3:
                print("Invalid JOINED message")
                return

            peer_node_id = parts[1]

            try:
                peer_port = int(parts[2])

            except ValueError:
                print("Invalid peer port")
                return


            # Add bootstrap peer
            self.peers[peer_node_id] = (
                addr[0],
                peer_port
            )

            print("Successfully joined peer")

            self.print_peers()


        # ==================================
        # GET_PEERS
        # ==================================

        elif message == "GET_PEERS":

            print("Peer list requested")

            self.send_peer_list(addr)


        # ==================================
        # PEERS
        # ==================================

        elif message.startswith("PEERS"):

            self.process_peer_list(message)


        # ==================================
        # STATS / GOSSIP
        # ==================================

        elif message.startswith("STATS|"):

            self.process_stats(
                message,
                addr
            )


        else:

            print("Unknown message:", message)


    # ======================================
    # Send JOIN request
    # ======================================

    def join_peer(self, peer_ip, peer_port):

        message = (
            f"JOIN|{self.node_id}|{self.port}"
        )

        self.transport.sendto(
            message.encode(),
            (peer_ip, peer_port)
        )

        print(
            f"\nSent JOIN to "
            f"{peer_ip}:{peer_port}"
        )


    # ======================================
    # Send PING
    # ======================================

    def ping_peer(self, peer_address):

        self.transport.sendto(
            b"PING",
            peer_address
        )

        print(
            f"Sent PING to {peer_address}"
        )


    # ======================================
    # Send peer list
    # ======================================

    def send_peer_list(self, addr):

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

        print(
            f"Sent peer list to {addr}"
        )


    # ======================================
    # Process peer list
    # ======================================

    def process_peer_list(self, message):

        parts = message.split("|")

        # First item is PEERS
        peer_entries = parts[1:]


        for entry in peer_entries:

            try:

                peer_id, ip, port = entry.split(",")

                port = int(port)

            except ValueError:

                print(
                    "Invalid peer entry:",
                    entry
                )

                continue


            # Do not add ourselves
            if peer_id == self.node_id:
                continue


            # Add new peer
            if peer_id not in self.peers:

                self.peers[peer_id] = (
                    ip,
                    port
                )

                print(
                    f"Discovered new peer: "
                    f"{peer_id} -> {ip}:{port}"
                )


        print("\nUpdated Peer Table:")

        self.print_peers()


    # ======================================
    # Kademlia-style closest peers
    # ======================================

    def get_closest_peers(self, count=3):

        sorted_peers = sorted(
            self.peers.items(),
            key=lambda item:
                xor_distance(
                    self.node_id,
                    item[0]
                )
        )

        return sorted_peers[:count]


    # ======================================
    # Print peers
    # ======================================

    def print_peers(self):

        if not self.peers:

            print("  No known peers")

            return


        for peer_id, address in self.peers.items():

            distance = xor_distance(
                self.node_id,
                peer_id
            )

            print(
                f"  {peer_id} -> "
                f"{address[0]}:{address[1]} "
                f"(XOR distance: {distance})"
            )


    # ======================================
    # Gossip Loop
    # ======================================

    async def gossip_loop(self):

        # Wait a little before first gossip
        await asyncio.sleep(2)


        while True:

            try:

                cpu = psutil.cpu_percent(
                    interval=None
                )

                ram = psutil.virtual_memory().percent


                # Gossip message
                message = (
                    f"STATS|"
                    f"{self.node_id}|"
                    f"{self.port}|"
                    f"{cpu:.2f}|"
                    f"{ram:.2f}"
                )


                # Send to known peers
                if self.peers:

                    print("\n------------------------------")
                    print("GOSSIP")
                    print("------------------------------")
                    print(
                        f"CPU: {cpu:.2f}%"
                    )
                    print(
                        f"RAM: {ram:.2f}%"
                    )


                    # Use closest peers
                    closest_peers = (
                        self.get_closest_peers(3)
                    )


                    for peer_id, address in closest_peers:

                        self.transport.sendto(
                            message.encode(),
                            address
                        )

                        print(
                            f"Sent stats to "
                            f"{peer_id}"
                        )

                else:

                    print(
                        "\nNo peers available "
                        "for gossip"
                    )


            except Exception as e:

                print(
                    "Gossip error:",
                    e
                )


            # Wait 5 seconds
            await asyncio.sleep(5)


    # ======================================
    # Process statistics
    # ======================================

    def process_stats(self, message, addr):

        parts = message.split("|")


        if len(parts) != 5:

            print(
                "Invalid STATS message"
            )

            return


        peer_id = parts[1]


        try:

            peer_port = int(parts[2])
            cpu = float(parts[3])
            ram = float(parts[4])

        except ValueError:

            print(
                "Invalid statistics"
            )

            return


        # Add peer if not known
        if peer_id != self.node_id:

            self.peers[peer_id] = (
                addr[0],
                peer_port
            )


        # Store statistics
        self.peer_stats[peer_id] = {

            "ip": addr[0],

            "port": peer_port,

            "cpu": cpu,

            "ram": ram
        }


        print("\n==============================")
        print("      PEER GOSSIP RECEIVED")
        print("==============================")

        print("Peer ID :", peer_id)
        print("Address :", addr[0])
        print("Port    :", peer_port)
        print(f"CPU     : {cpu:.2f}%")
        print(f"RAM     : {ram:.2f}%")

        print("==============================")


    # ======================================
    # Connection lost
    # ======================================

    def connection_lost(self, exc):

        print(
            "\nNode connection closed"
        )

        if self.gossip_task:

            self.gossip_task.cancel()


# ==========================================
# Start Node
# ==========================================

async def start_node(
    port,
    bootstrap_ip=None,
    bootstrap_port=None
):

    loop = asyncio.get_running_loop()


    transport, protocol = (
        await loop.create_datagram_endpoint(
            lambda: DHTNode(port),
            local_addr=(
                "127.0.0.1",
                port
            )
        )
    )


    # Give the UDP server a moment to start
    await asyncio.sleep(1)


    # Join bootstrap node
    if (
        bootstrap_ip is not None
        and bootstrap_port is not None
    ):

        protocol.join_peer(
            bootstrap_ip,
            bootstrap_port
        )


    try:

        await asyncio.Future()


    finally:

        transport.close()


# ==========================================
# Main
# ==========================================

if __name__ == "__main__":

    if len(sys.argv) not in (2, 4):

        print("Usage:")
        print(
            "python dht_node.py <port>"
        )

        print(
            "python dht_node.py "
            "<port> <bootstrap_ip> "
            "<bootstrap_port>"
        )

        print("\nExamples:")

        print(
            "python dht_node.py 8001"
        )

        print(
            "python dht_node.py "
            "8002 127.0.0.1 8001"
        )

        sys.exit(1)


    try:

        port = int(sys.argv[1])


        if len(sys.argv) == 4:

            bootstrap_ip = sys.argv[2]

            bootstrap_port = int(
                sys.argv[3]
            )

        else:

            bootstrap_ip = None
            bootstrap_port = None


    except ValueError:

        print(
            "Port must be a number"
        )

        sys.exit(1)


    asyncio.run(
        start_node(
            port,
            bootstrap_ip,
            bootstrap_port
        )
    )