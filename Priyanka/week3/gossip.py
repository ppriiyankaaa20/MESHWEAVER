import asyncio
import json
import time
import psutil


class GossipProtocol:

    def __init__(self, node_id, transport, discovery):

        self.node_id = node_id
        self.transport = transport
        self.discovery = discovery

        # Stores CPU/RAM information of other nodes
        self.peer_status = {}

        # Stores last time we heard from each node
        self.last_seen = {}

        self.interval = 5
        self.timeout = 12

    # ============================================================
    # GET CPU AND RAM
    # ============================================================

    def get_system_status(self):

        cpu = psutil.cpu_percent(interval=None)
        memory = psutil.virtual_memory().percent

        return cpu, memory

    # ============================================================
    # SEND GOSSIP
    # ============================================================

    async def gossip(self):

        print(
            f"[GOSSIP] Node {self.node_id} gossip started"
        )

        while True:

            try:

                cpu, memory = self.get_system_status()

                message = {
                    "type": "GOSSIP",
                    "node_id": self.node_id,
                    "cpu": cpu,
                    "memory": memory
                }

                data = json.dumps(message).encode()

                peers = self.discovery.get_peers()

                for peer in peers:

                    peer = tuple(peer)

                    # Never send gossip to ourselves
                    if peer[1] == self.discovery.port:
                        continue

                    print(
                        f"[GOSSIP] {self.node_id} -> {peer}"
                    )

                    self.transport.sendto(
                        data,
                        peer
                    )

            except Exception as e:

                print(
                    f"[GOSSIP ERROR] {e}"
                )

            await asyncio.sleep(
                self.interval
            )

    # ============================================================
    # RECEIVE GOSSIP
    # ============================================================

    def receive_gossip(self, message, addr):

        try:

            node_id = message["node_id"]
            cpu = float(message["cpu"])
            memory = float(message["memory"])

            # Ignore our own gossip
            if node_id == self.node_id:
                return

            # Save node information
            self.peer_status[node_id] = {
                "address": tuple(addr),
                "cpu": cpu,
                "memory": memory
            }

            # Update heartbeat
            self.last_seen[node_id] = time.time()

            print(
                f"[GOSSIP RECEIVED] "
                f"{node_id} -> Node {self.node_id} | "
                f"CPU={cpu}% | RAM={memory}%"
            )

        except Exception as e:

            print(
                f"[GOSSIP RECEIVE ERROR] {e}"
            )

    # ============================================================
    # REMOVE DEAD NODES
    # ============================================================

    def remove_dead_peers(self):

        current_time = time.time()

        dead_nodes = []

        for node_id in list(
            self.peer_status.keys()
        ):

            last = self.last_seen.get(
                node_id,
                0
            )

            if current_time - last > self.timeout:

                dead_nodes.append(
                    node_id
                )

        for node_id in dead_nodes:

            print(
                f"[FAULT] Node {node_id} "
                f"has gone offline."
            )

            self.peer_status.pop(
                node_id,
                None
            )

            self.last_seen.pop(
                node_id,
                None
            )

    # ============================================================
    # SHOW PEER STATUS
    # ============================================================

    def show_peer_status(self):

        self.remove_dead_peers()

        print(
            "\n========== PEER STATUS =========="
        )

        if not self.peer_status:

            print(
                "No peer information available."
            )

        else:

            current_time = time.time()

            for node_id, status in self.peer_status.items():

                last = self.last_seen.get(
                    node_id,
                    0
                )

                age = current_time - last

                print(
                    f"Node: {node_id} | "
                    f"CPU: {status['cpu']}% | "
                    f"RAM: {status['memory']}% | "
                    f"Last seen: {age:.1f}s ago"
                )

        print(
            "=================================\n"
        )

    # ============================================================
    # GET DISCOVERED PEERS
    # ============================================================

    def get_discovered_peers(self):

        return self.discovery.get_peers()