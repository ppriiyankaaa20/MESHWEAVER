# MeshWeaver — Week 3 DHT Node

import asyncio
import hashlib
import socket
import sys
import psutil
import uuid
import time


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

        # --------------------------------------
        # Known peers
        # node_id -> (ip, port)
        # --------------------------------------

        self.peers = {}

        # --------------------------------------
        # Peer statistics
        # node_id -> stats
        # --------------------------------------

        self.peer_stats = {}

        # --------------------------------------
        # Last heartbeat received
        # node_id -> timestamp
        # --------------------------------------

        self.last_seen = {}

        # --------------------------------------
        # Peer status
        # node_id -> ONLINE/OFFLINE
        # --------------------------------------

        self.peer_status = {}

        # --------------------------------------
        # Tasks
        # task_id -> task information
        # --------------------------------------

        self.tasks = {}

        self.transport = None

        self.gossip_task = None
        self.heartbeat_task = None


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

        # Start background tasks

        self.gossip_task = asyncio.create_task(
            self.gossip_loop()
        )

        self.heartbeat_task = asyncio.create_task(
            self.heartbeat_loop()
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


        print(
            f"\nReceived from {addr}: {message}"
        )


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

            self.handle_pong(addr)


        # ==================================
        # JOIN
        # ==================================

        elif message.startswith("JOIN|"):

            self.handle_join(
                message,
                addr
            )


        # ==================================
        # JOINED
        # ==================================

        elif message.startswith("JOINED|"):

            self.handle_joined(
                message,
                addr
            )


        # ==================================
        # PEERS
        # ==================================

        elif message.startswith("PEERS"):

            self.process_peer_list(
                message
            )


        # ==================================
        # STATS
        # ==================================

        elif message.startswith("STATS|"):

            self.process_stats(
                message,
                addr
            )


        # ==================================
        # TASK
        # ==================================

        elif message.startswith("TASK|"):

            self.process_task(
                message,
                addr
            )


        # ==================================
        # TASK RESULT
        # ==================================

        elif message.startswith("TASK_RESULT|"):

            self.process_task_result(
                message
            )


        # ==================================
        # TASK FAILED
        # ==================================

        elif message.startswith("TASK_FAILED|"):

            self.process_task_failed(
                message
            )


        else:

            print(
                "Unknown message:",
                message
            )


    # ======================================
    # JOIN
    # ======================================

    def handle_join(self, message, addr):

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


        self.peers[joining_node_id] = (
            addr[0],
            joining_port
        )

        self.peer_status[joining_node_id] = "ONLINE"
        self.last_seen[joining_node_id] = time.time()


        print("Join request received")

        self.print_peers()


        response = (
            f"JOINED|{self.node_id}|{self.port}"
        )

        self.transport.sendto(
            response.encode(),
            addr
        )


        self.send_peer_list(addr)


    # ======================================
    # JOINED
    # ======================================

    def handle_joined(self, message, addr):

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


        self.peers[peer_node_id] = (
            addr[0],
            peer_port
        )

        self.peer_status[peer_node_id] = "ONLINE"
        self.last_seen[peer_node_id] = time.time()


        print(
            "Successfully joined peer"
        )

        self.print_peers()


    # ======================================
    # JOIN PEER
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

    def ping_peer(self, peer_id, address):

        self.transport.sendto(
            b"PING",
            address
        )

        print(
            f"Heartbeat PING -> "
            f"{peer_id}"
        )


    # ======================================
    # Handle PONG
    # ======================================

    def handle_pong(self, addr):

       for peer_id, address in self.peers.items():

        if address[0] == addr[0]:

            self.last_seen[peer_id] = time.time()

            self.peer_status[peer_id] = "ONLINE"

            print(
                f"Heartbeat OK: {peer_id}"
            )

            return


    # ======================================
    # Heartbeat Loop
    # ======================================

    async def heartbeat_loop(self):

        await asyncio.sleep(3)

        while True:

            try:

                current_time = time.time()

                for peer_id, address in list(
                    self.peers.items()
                ):

                    self.ping_peer(
                        peer_id,
                        address
                    )

                    last = self.last_seen.get(
                        peer_id,
                        current_time
                    )

                    # --------------------------
                    # Offline after 10 seconds
                    # --------------------------

                    if (
                        current_time - last
                        > 10
                    ):

                        if (
                            self.peer_status.get(
                                peer_id
                            )
                            != "OFFLINE"
                        ):

                            print(
                                "\n!!! PEER OFFLINE !!!"
                            )

                            print(
                                "Peer:",
                                peer_id
                            )

                            self.peer_status[
                                peer_id
                            ] = "OFFLINE"

                            self.handle_failed_peer(
                                peer_id
                            )

            except Exception as e:

                print(
                    "Heartbeat error:",
                    e
                )

            await asyncio.sleep(5)


    # ======================================
    # Handle Failed Peer
    # ======================================

    def handle_failed_peer(self, peer_id):

        print(
            f"Checking tasks assigned to "
            f"failed peer {peer_id}"
        )


        for task_id, task in self.tasks.items():

            if (
                task["node_id"] == peer_id
                and task["status"] == "RUNNING"
            ):

                print(
                    f"Task {task_id} failed"
                )

                task["status"] = "FAILED"

                # Try another node

                self.route_task(
                    task_id,
                    task["payload"]
                )


    # ======================================
    # Send Peer List
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


    # ======================================
    # Process Peer List
    # ======================================

    def process_peer_list(self, message):

        parts = message.split("|")

        peer_entries = parts[1:]


        for entry in peer_entries:

            try:

                peer_id, ip, port = (
                    entry.split(",")
                )

                port = int(port)

            except ValueError:

                continue


            if peer_id == self.node_id:

                continue


            self.peers[peer_id] = (
                ip,
                port
            )

            self.peer_status[peer_id] = "ONLINE"

            self.last_seen[peer_id] = time.time()


        print(
            "\nUpdated Peer Table:"
        )

        self.print_peers()


    # ======================================
    # Process Stats
    # ======================================

    def process_stats(self, message, addr):

        parts = message.split("|")

        if len(parts) != 5:

            return


        peer_id = parts[1]

        try:

            peer_port = int(parts[2])
            cpu = float(parts[3])
            ram = float(parts[4])

        except ValueError:

            return


        if peer_id != self.node_id:

            self.peers[peer_id] = (
                addr[0],
                peer_port
            )


        self.peer_stats[peer_id] = {

            "ip": addr[0],
            "port": peer_port,
            "cpu": cpu,
            "ram": ram
        }


        self.last_seen[peer_id] = time.time()

        self.peer_status[peer_id] = "ONLINE"


        print(
            f"Peer {peer_id} | "
            f"CPU={cpu:.2f}% | "
            f"RAM={ram:.2f}%"
        )


    # ======================================
    # Gossip Loop
    # ======================================

    async def gossip_loop(self):

        await asyncio.sleep(2)

        while True:

            try:

                cpu = psutil.cpu_percent(
                    interval=None
                )

                ram = psutil.virtual_memory().percent


                message = (
                    f"STATS|"
                    f"{self.node_id}|"
                    f"{self.port}|"
                    f"{cpu:.2f}|"
                    f"{ram:.2f}"
                )


                closest_peers = (
                    self.get_closest_peers(3)
                )


                for peer_id, address in closest_peers:

                    if (
                        self.peer_status.get(
                            peer_id,
                            "ONLINE"
                        )
                        == "ONLINE"
                    ):

                        self.transport.sendto(
                            message.encode(),
                            address
                        )


            except Exception as e:

                print(
                    "Gossip error:",
                    e
                )


            await asyncio.sleep(5)


    # ======================================
    # Closest Peers
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
    # TASK SUBMISSION
    # ======================================

    def submit_task(self, payload):

        task_id = str(uuid.uuid4())

        self.tasks[task_id] = {

            "task_id": task_id,

            "payload": payload,

            "node_id": None,

            "status": "SUBMITTED"
        }


        print(
            "\n================================"
        )

        print(
            "TASK SUBMITTED"
        )

        print(
            "Task ID:",
            task_id
        )

        print(
            "Payload:",
            payload
        )

        print(
            "================================"
        )


        self.route_task(
            task_id,
            payload
        )


    # ======================================
    # Select Lowest CPU Node
    # ======================================

    def select_lowest_cpu_node(self):

        available_nodes = []


        # Include ourselves

        own_cpu = psutil.cpu_percent(
            interval=None
        )

        available_nodes.append(
            (
                self.node_id,
                (
                    self.host,
                    self.port
                ),
                own_cpu
            )
        )


        # Add online peers

        for peer_id, stats in self.peer_stats.items():

            if (
                self.peer_status.get(
                    peer_id
                )
                == "ONLINE"
            ):

                available_nodes.append(
                    (
                        peer_id,
                        (
                            stats["ip"],
                            stats["port"]
                        ),
                        stats["cpu"]
                    )
                )


        if not available_nodes:

            return None


        selected = min(
            available_nodes,
            key=lambda item: item[2]
        )


        print(
            "\nLowest CPU node:"
        )

        print(
            "Node ID:",
            selected[0]
        )

        print(
            "CPU:",
            selected[2]
        )


        return selected


    # ======================================
    # Route Task
    # ======================================

    def route_task(self, task_id, payload):

        selected = self.select_lowest_cpu_node()


        if selected is None:

            print(
                "No available node"
            )

            return


        node_id, address, cpu = selected


        self.tasks[task_id]["node_id"] = (
            node_id
        )

        self.tasks[task_id]["status"] = (
            "RUNNING"
        )


        # ----------------------------------
        # Execute locally
        # ----------------------------------

        if node_id == self.node_id:

            print(
                f"Executing task {task_id} locally"
            )

            asyncio.create_task(
                self.execute_local_task(
                    task_id,
                    payload
                )
            )

            return


        # ----------------------------------
        # Send task to remote node
        # ----------------------------------

        message = (
            f"TASK|"
            f"{task_id}|"
            f"{payload}"
        )


        self.transport.sendto(
            message.encode(),
            address
        )


        print(
            f"Task {task_id} routed to "
            f"{node_id} "
            f"(CPU {cpu:.2f}%)"
        )


    # ======================================
    # Execute Local Task
    # ======================================

    async def execute_local_task(
        self,
        task_id,
        payload
    ):

        await asyncio.sleep(2)

        result = (
            f"Processed: {payload}"
        )


        self.tasks[task_id]["status"] = (
            "COMPLETED"
        )

        self.tasks[task_id]["result"] = (
            result
        )


        print(
            "\nTASK COMPLETED"
        )

        print(
            "Task ID:",
            task_id
        )

        print(
            "Result:",
            result
        )


    # ======================================
    # Process Remote Task
    # ======================================

    def process_task(self, message, addr):

        parts = message.split("|", 2)

        if len(parts) != 3:

            return


        task_id = parts[1]

        payload = parts[2]


        print(
            "\n================================"
        )

        print(
            "TASK RECEIVED"
        )

        print(
            "Task ID:",
            task_id
        )

        print(
            "Payload:",
            payload
        )

        print(
            "================================"
        )


        asyncio.create_task(
            self.execute_remote_task(
                task_id,
                payload,
                addr
            )
        )


    # ======================================
    # Execute Remote Task
    # ======================================

    async def execute_remote_task(
        self,
        task_id,
        payload,
        addr
    ):

        await asyncio.sleep(3)


        result = (
            f"Processed by "
            f"{self.node_id}: "
            f"{payload}"
        )


        message = (
            f"TASK_RESULT|"
            f"{task_id}|"
            f"{result}"
        )


        self.transport.sendto(
            message.encode(),
            addr
        )


        print(
            f"Task result sent: {task_id}"
        )


    # ======================================
    # Process Task Result
    # ======================================

    def process_task_result(self, message):

        parts = message.split("|", 2)

        if len(parts) != 3:

            return


        task_id = parts[1]

        result = parts[2]


        if task_id in self.tasks:

            self.tasks[task_id]["status"] = (
                "COMPLETED"
            )

            self.tasks[task_id]["result"] = (
                result
            )


        print(
            "\n================================"
        )

        print(
            "TASK COMPLETED"
        )

        print(
            "Task ID:",
            task_id
        )

        print(
            "Result:",
            result
        )

        print(
            "================================"
        )


    # ======================================
    # Process Failed Task
    # ======================================

    def process_task_failed(self, message):

        parts = message.split("|", 1)

        if len(parts) != 2:

            return


        task_id = parts[1]


        if task_id in self.tasks:

            self.tasks[task_id]["status"] = (
                "FAILED"
            )


        print(
            f"Task failed: {task_id}"
        )


    # ======================================
    # Print Peers
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

            status = self.peer_status.get(
                peer_id,
                "UNKNOWN"
            )


            print(
                f"  {peer_id} -> "
                f"{address[0]}:{address[1]} "
                f"[{status}] "
                f"(XOR: {distance})"
            )


    # ======================================
    # Connection Lost
    # ======================================

    def connection_lost(self, exc):

        print(
            "\nNode connection closed"
        )


        if self.gossip_task:

            self.gossip_task.cancel()


        if self.heartbeat_task:

            self.heartbeat_task.cancel()


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


    await asyncio.sleep(1)


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
