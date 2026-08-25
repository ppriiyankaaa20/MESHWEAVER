import asyncio
import json
import psutil


class GossipProtocol:

    def __init__(self, node_id, transport, discovery):
        self.node_id = node_id
        self.transport = transport
        self.discovery = discovery

        # Store information about other nodes
        self.peer_status = {}

    def get_system_status(self):

        cpu = psutil.cpu_percent(interval=None)
        memory = psutil.virtual_memory().percent

        return {
            "cpu": cpu,
            "memory": memory
        }

    async def gossip(self):

        while True:

            status = self.get_system_status()

            message = {
                "type": "GOSSIP",
                "node_id": self.node_id,
                "cpu": status["cpu"],
                "memory": status["memory"]
            }

            data = json.dumps(message).encode()

            peers = self.discovery.get_peers()

            for peer in peers:

                try:

                    self.transport.sendto(
                        data,
                        tuple(peer)
                    )

                except Exception as e:

                    print(
                        f"[GOSSIP ERROR] "
                        f"Could not contact {peer}: {e}"
                    )

            await asyncio.sleep(5)

    def receive_gossip(self, message, addr):

        node_id = message["node_id"]

        cpu = message["cpu"]
        memory = message["memory"]

        self.peer_status[node_id] = {
            "address": addr,
            "cpu": cpu,
            "memory": memory
        }

        print(
            f"[GOSSIP] {node_id} "
            f"CPU={cpu}% "
            f"RAM={memory}%"
        )

    def show_peer_status(self):

        print("\n========== PEER STATUS ==========")

        if not self.peer_status:

            print("No peer information available.")

        else:

            for node_id, status in self.peer_status.items():

                print(
                    f"Node: {node_id} | "
                    f"CPU: {status['cpu']}% | "
                    f"RAM: {status['memory']}%"
                )

        print("=================================\n")