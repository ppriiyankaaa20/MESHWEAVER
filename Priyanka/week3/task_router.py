import asyncio
import cloudpickle
import time


class TaskRouter:

    def __init__(
        self,
        node_id,
        transport,
        gossip
    ):

        self.node_id = node_id
        self.transport = transport
        self.gossip = gossip

        self.pending_acks = {}

        self.peer_timeout = 12
        self.ack_timeout = 3

    # ============================================================
    # GET HEALTHY WORKERS
    # ============================================================

    def get_healthy_peers(self):

        current_time = time.time()

        healthy = []

        for node_id, status in (
            self.gossip.peer_status.items()
        ):

            if node_id == self.node_id:

                continue

            last_seen = self.gossip.last_seen.get(
                node_id,
                0
            )

            if current_time - last_seen <= self.peer_timeout:

                healthy.append(
                    (node_id, status)
                )

        return healthy

    # ============================================================
    # SELECT LOWEST CPU NODE
    # ============================================================

    def select_best_node(
        self,
        excluded_nodes=None
    ):

        if excluded_nodes is None:

            excluded_nodes = set()

        candidates = []

        for node_id, status in (
            self.get_healthy_peers()
        ):

            if node_id not in excluded_nodes:

                candidates.append(
                    (node_id, status)
                )

        if not candidates:

            return None

        # Select node with lowest CPU
        return min(
            candidates,
            key=lambda item: item[1]["cpu"]
        )

    # ============================================================
    # HANDLE ACK
    # ============================================================

    def handle_ack(self, task_id):

        future = self.pending_acks.get(
            task_id
        )

        if future and not future.done():

            future.set_result(
                True
            )

    # ============================================================
    # ROUTE TASK
    # ============================================================

    async def route_task(self, data):

        try:

            task_package = cloudpickle.loads(
                data
            )

            task_id = task_package[
                "task_id"
            ]

        except Exception as e:

            print(
                f"[ROUTER ERROR] Invalid task: {e}"
            )

            return

        tried_nodes = set()

        while True:

            selected = self.select_best_node(
                tried_nodes
            )

            # ----------------------------------------------------
            # No workers
            # ----------------------------------------------------

            if selected is None:

                print(
                    "\n[ROUTER] "
                    "No healthy worker node available."
                )

                return

            node_id, status = selected

            address = tuple(
                status["address"]
            )

            print(
                "\n========== TASK ROUTING =========="
            )

            print(
                f"Task ID      : {task_id}"
            )

            print(
                f"Selected Node: {node_id}"
            )

            print(
                f"CPU Usage    : {status['cpu']}%"
            )

            print(
                f"RAM Usage    : {status['memory']}%"
            )

            print(
                f"Address      : {address}"
            )

            print(
                "=================================="
            )

            # Add node to tried list
            tried_nodes.add(
                node_id
            )

            # Create ACK future
            loop = asyncio.get_running_loop()

            ack_future = loop.create_future()

            self.pending_acks[
                task_id
            ] = ack_future

            # ----------------------------------------------------
            # Tell worker to execute
            # ----------------------------------------------------

            task_package[
                "execute_here"
            ] = True

            task_package[
                "router_addr"
            ] = (
                "127.0.0.1",
                self.gossip.discovery.port
            )

            updated_data = cloudpickle.dumps(
                task_package
            )

            try:

                self.transport.sendto(
                    updated_data,
                    address
                )

                print(
                    f"[ROUTER] Task sent to Node {node_id}"
                )

                # Wait for worker ACK
                await asyncio.wait_for(
                    ack_future,
                    timeout=self.ack_timeout
                )

                print(
                    f"[ROUTER] "
                    f"Node {node_id} accepted task."
                )

                self.pending_acks.pop(
                    task_id,
                    None
                )

                return

            except asyncio.TimeoutError:

                print(
                    f"[FAULT] "
                    f"Node {node_id} did not respond."
                )

                print(
                    "[ROUTER] Re-routing task..."
                )

                self.pending_acks.pop(
                    task_id,
                    None
                )

            except Exception as e:

                print(
                    f"[ROUTER ERROR] {e}"
                )

                self.pending_acks.pop(
                    task_id,
                    None
                )

                return