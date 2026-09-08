import asyncio
from dht_node import DHTNode


# ==========================================
# Configuration
# ==========================================

HOST = "127.0.0.1"

NODE_PORTS = [
    8001,
    8002,
    8003,
    8004,
    8005,
    8006,
    8007,
    8008,
    8009,
    8010,
]

BOOTSTRAP_PORT = 8001


# ==========================================
# Task Router
# ==========================================

class TaskRouter:

    def __init__(self, nodes):

        self.nodes = nodes

        self.task_counter = 0


    # ======================================
    # Find node with lowest CPU
    # ======================================

    def find_lowest_cpu_node(self):

        candidates = []

        for port, protocol in self.nodes.items():

            # Ignore ourselves / nodes without statistics
            if not protocol.peer_stats:
                continue

            # Find this node's own latest CPU information
            own_stats = protocol.peer_stats.get(
                protocol.node_id
            )

            if own_stats:

                cpu = own_stats["cpu"]

                candidates.append(
                    (cpu, port, protocol.node_id)
                )


        # If we don't have self CPU statistics,
        # use peer statistics as fallback.

        if not candidates:

            for port, protocol in self.nodes.items():

                for node_id, stats in protocol.peer_stats.items():

                    if node_id == protocol.node_id:
                        continue

                    cpu = stats["cpu"]

                    candidates.append(
                        (cpu, port, node_id)
                    )


        if not candidates:

            return None


        # Lowest CPU first
        candidates.sort(
            key=lambda item: item[0]
        )

        return candidates[0]


    # ======================================
    # Submit task
    # ======================================

    def submit_task(self, task_data):

        selected = self.find_lowest_cpu_node()

        if selected is None:

            print(
                "\n[ROUTER] No CPU information available."
            )

            return None


        cpu, port, node_id = selected

        self.task_counter += 1

        task_id = f"TASK-{self.task_counter:04d}"


        print("\n")
        print("=" * 60)
        print("                 TASK ROUTER")
        print("=" * 60)

        print("Task ID       :", task_id)

        print("Selected Node :", port)

        print("Node ID       :", node_id)

        print(f"Reported CPU  : {cpu:.2f}%")

        print("Task          :", task_data)

        print("=" * 60)


        return {
            "task_id": task_id,
            "node_port": port,
            "node_id": node_id,
            "cpu": cpu,
            "data": task_data,
        }


# ==========================================
# Start DHT node
# ==========================================

async def start_node(port, bootstrap_port=None):

    loop = asyncio.get_running_loop()

    transport, protocol = (
        await loop.create_datagram_endpoint(
            lambda: DHTNode(port),
            local_addr=(HOST, port)
        )
    )

    print(
        f"[NODE STARTED] {port}"
    )

    await asyncio.sleep(0.5)

    if bootstrap_port is not None:

        protocol.join_peer(
            HOST,
            bootstrap_port
        )

        print(
            f"[JOIN] {port} -> {bootstrap_port}"
        )

    return transport, protocol


# ==========================================
# Main test
# ==========================================

async def main():

    print("\n")
    print("=" * 60)
    print("          WEEK 3 TASK ROUTING TEST")
    print("=" * 60)


    nodes = {}


    # --------------------------------------
    # Start 10 nodes
    # --------------------------------------

    for port in NODE_PORTS:

        bootstrap = (
            None
            if port == BOOTSTRAP_PORT
            else BOOTSTRAP_PORT
        )

        transport, protocol = await start_node(
            port,
            bootstrap
        )

        nodes[port] = protocol

        await asyncio.sleep(0.5)


    # --------------------------------------
    # Allow gossip to collect CPU data
    # --------------------------------------

    print("\n")
    print("Waiting for CPU statistics...")

    await asyncio.sleep(8)


    # --------------------------------------
    # Show CPU information
    # --------------------------------------

    print("\n")
    print("=" * 60)
    print("             CURRENT NODE LOADS")
    print("=" * 60)

    for port, protocol in nodes.items():

        print(
            f"Node {port}: "
            f"{len(protocol.peer_stats)} statistics received"
        )


    # --------------------------------------
    # Create router
    # --------------------------------------

    router = TaskRouter(nodes)


    # --------------------------------------
    # Submit test task
    # --------------------------------------

    task = {
        "operation": "ADD",
        "arguments": [10, 20]
    }


    result = router.submit_task(task)


    # --------------------------------------
    # Result
    # --------------------------------------

    if result:

        print("\n")
        print("=" * 60)
        print("                 ROUTING RESULT")
        print("=" * 60)

        print(
            "[PASS] Task routed successfully."
        )

        print(
            "Task ID      :",
            result["task_id"]
        )

        print(
            "Selected Node:",
            result["node_port"]
        )

        print(
            f"CPU          : {result['cpu']:.2f}%"
        )

    else:

        print(
            "\n[FAIL] Could not route task."
        )


    # --------------------------------------
    # Keep network alive briefly
    # --------------------------------------

    print("\nWaiting before shutdown...")

    await asyncio.sleep(5)


    # --------------------------------------
    # Shutdown
    # --------------------------------------

    print("\n")
    print("=" * 60)
    print("              SHUTTING DOWN")
    print("=" * 60)

    for port, protocol in nodes.items():

        if protocol.transport:

            protocol.transport.close()

        print(
            f"[STOPPED] Node {port}"
        )


# ==========================================
# Run
# ==========================================

if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print(
            "\nTest interrupted."
        )