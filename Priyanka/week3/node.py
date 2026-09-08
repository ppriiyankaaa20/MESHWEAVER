import asyncio
import json
import cloudpickle

from discovery import PeerDiscovery
from gossip import GossipProtocol
from task_router import TaskRouter
from task_executor import TaskExecutor


class MeshNode(asyncio.DatagramProtocol):

    def __init__(
        self,
        node_id,
        port,
        known_peers
    ):

        self.node_id = node_id
        self.port = port
        self.known_peers = known_peers

        self.transport = None

        self.discovery = None
        self.gossip = None
        self.router = None
        self.executor = None

    # ============================================================
    # CONNECTION
    # ============================================================

    def connection_made(
        self,
        transport
    ):

        self.transport = transport

        print("\n" + "=" * 55)
        print("              MESHWEAVER WEEK 3")
        print("=" * 55)

        print(
            f"Node ID : {self.node_id}"
        )

        print(
            f"Port    : {self.port}"
        )

        print(
            "=" * 55
        )

    # ============================================================
    # RECEIVE UDP MESSAGE
    # ============================================================

    def datagram_received(
        self,
        data,
        addr
    ):

        # ========================================================
        # FIRST: TRY JSON
        # ========================================================

        try:

            message = json.loads(
                data.decode()
            )

            message_type = message.get(
                "type"
            )

            # ----------------------------------------------------
            # GOSSIP
            # ----------------------------------------------------

            if message_type == "GOSSIP":

                self.gossip.receive_gossip(
                    message,
                    addr
                )

                return

            # ----------------------------------------------------
            # DISCOVERY
            # ----------------------------------------------------

            if message_type in (
                "DISCOVER",
                "PEER_LIST"
            ):

                self.discovery.handle_message(
                    message,
                    addr
                )

                return

        except Exception:

            # Not JSON.
            # It may be a cloudpickle task.
            pass

        # ========================================================
        # SECOND: TRY CLOUDPICKLE
        # ========================================================

        try:

            message = cloudpickle.loads(
                data
            )

            message_type = message.get(
                "type"
            )

            # ----------------------------------------------------
            # TASK ACK
            # ----------------------------------------------------

            if message_type == "TASK_ACK":

                task_id = message.get(
                    "task_id"
                )

                print(
                    f"[ACK] Received ACK for task {task_id}"
                )

                self.router.handle_ack(
                    task_id
                )

                return

            # ----------------------------------------------------
            # TASK RESULT
            # ----------------------------------------------------

            if message_type == "TASK_RESULT":

                reply_addr = message.get(
                    "reply_addr"
                )

                if reply_addr:

                    self.transport.sendto(
                        data,
                        tuple(reply_addr)
                    )

                return

            # ----------------------------------------------------
            # TASK
            # ----------------------------------------------------

            if "task_id" in message:

                task_id = message[
                    "task_id"
                ]

                execute_here = message.get(
                    "execute_here",
                    False
                )

                # ------------------------------------------------
                # Worker
                # ------------------------------------------------

                if execute_here:

                    print(
                        f"\n[WORKER] "
                        f"Task {task_id} assigned "
                        f"to Node {self.node_id}"
                    )

                    asyncio.create_task(
                        self.execute_and_ack(
                            data,
                            addr
                        )
                    )

                # ------------------------------------------------
                # Router
                # ------------------------------------------------

                else:

                    print(
                        f"\n[ROUTER] "
                        f"Task {task_id} received"
                    )

                    asyncio.create_task(
                        self.router.route_task(
                            data
                        )
                    )

                return

        except Exception as e:

            print(
                f"[NODE ERROR] {e}"
            )

    # ============================================================
    # EXECUTE + ACK
    # ============================================================

    async def execute_and_ack(
        self,
        data,
        router_addr
    ):

        try:

            task_package = cloudpickle.loads(
                data
            )

            task_id = task_package[
                "task_id"
            ]

            # Execute task
            await self.executor.execute_task(
                data
            )

            # ----------------------------------------------------
            # Send ACK to router
            # ----------------------------------------------------

            ack = {

                "type": "TASK_ACK",

                "task_id": task_id,

                "worker": self.node_id
            }

            ack_data = cloudpickle.dumps(
                ack
            )

            self.transport.sendto(
                ack_data,
                router_addr
            )

        except Exception as e:

            print(
                f"[EXECUTION ERROR] {e}"
            )

    # ============================================================
    # START SERVICES
    # ============================================================

    async def start_services(self):

        # --------------------------------------------------------
        # Discovery
        # --------------------------------------------------------

        self.discovery = PeerDiscovery(
            self.node_id,
            self.port,
            self.known_peers
        )

        self.discovery.set_transport(
            self.transport
        )

        # --------------------------------------------------------
        # Gossip
        # --------------------------------------------------------

        self.gossip = GossipProtocol(
            self.node_id,
            self.transport,
            self.discovery
        )

        # --------------------------------------------------------
        # Router
        # --------------------------------------------------------

        self.router = TaskRouter(
            self.node_id,
            self.transport,
            self.gossip
        )

        # --------------------------------------------------------
        # Executor
        # --------------------------------------------------------

        self.executor = TaskExecutor(
            self.node_id,
            self.transport
        )

        print(
            f"[STARTUP] Node {self.node_id} "
            "services initialized."
        )

        # Give other nodes time to start
        await asyncio.sleep(
            1
        )

        # --------------------------------------------------------
        # Discover known peers
        # --------------------------------------------------------

        for peer in self.known_peers:

            self.discovery.discover_peer(
                tuple(peer)
            )

        # --------------------------------------------------------
        # Start gossip
        # --------------------------------------------------------

        asyncio.create_task(
            self.gossip.gossip()
        )

        # --------------------------------------------------------
        # Display status
        # --------------------------------------------------------

        while True:

            await asyncio.sleep(
                10
            )

            self.gossip.show_peer_status()


# ================================================================
# START NODE
# ================================================================

async def start_node(
    node_id,
    port,
    known_peers
):

    loop = asyncio.get_running_loop()

    transport, protocol = (
        await loop.create_datagram_endpoint(

            lambda: MeshNode(
                node_id,
                port,
                known_peers
            ),

            local_addr=(
                "127.0.0.1",
                port
            )
        )
    )

    try:

        await protocol.start_services()

    except asyncio.CancelledError:

        pass

    finally:

        transport.close()


# ================================================================
# MAIN
# ================================================================

if __name__ == "__main__":

    import sys

    if len(sys.argv) < 3:

        print(
            "Usage:"
        )

        print(
            "py node.py <node_id> <port> [peer_port ...]"
        )

        sys.exit(1)

    node_id = sys.argv[1]

    port = int(
        sys.argv[2]
    )

    known_peers = []

    for peer_port in sys.argv[3:]:

        known_peers.append(
            (
                "127.0.0.1",
                int(peer_port)
            )
        )

    asyncio.run(
        start_node(
            node_id,
            port,
            known_peers
        )
    )