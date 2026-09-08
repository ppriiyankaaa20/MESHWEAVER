#!/usr/bin/env python3

"""
===============================================================
                    MESHWEAVER
          Zero-Dependency P2P Async Task Broker
===============================================================

WEEK 1
------
- Async UDP networking
- Client / server communication
- cloudpickle serialization
- Remote Python function execution

WEEK 2
------
- Peer discovery
- Kademlia-style peer table
- CPU/RAM gossip every 5 seconds

WEEK 3
------
- Lowest-CPU worker selection
- Heartbeat monitoring
- Fault detection
- Automatic task rerouting

WEEK 4
------
- HMAC cryptographic signatures
- Secure task validation
- CLI
- Live mesh dashboard

===============================================================
COMMANDS
===============================================================

Node A:
    py -3.11 meshweaver.py node A 9001

Node B:
    py -3.11 meshweaver.py node B 9002 --peer 9001

Node C:
    py -3.11 meshweaver.py node C 9003 --peer 9001

Send task:
    py -3.11 meshweaver.py send 9001

===============================================================
"""

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import sys
import time
import uuid

import cloudpickle
import psutil


# ===============================================================
# CONFIGURATION
# ===============================================================

GOSSIP_INTERVAL = 5
HEARTBEAT_INTERVAL = 3
PEER_TIMEOUT = 10
TASK_TIMEOUT = 4

SHARED_SECRET = b"meshweaver-secret-key"


# ===============================================================
# COLORS
# ===============================================================

RESET = "\033[0m"
BOLD = "\033[1m"

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
CYAN = "\033[96m"


# ===============================================================
# UTILITY FUNCTIONS
# ===============================================================

def current_time():
    return time.time()


def time_string():
    return time.strftime("%H:%M:%S")


def create_task_id():
    return str(uuid.uuid4())[:8]


def get_system_status():

    return {
        "cpu": psutil.cpu_percent(interval=None),
        "memory": psutil.virtual_memory().percent
    }


# ===============================================================
# SECURITY
# ===============================================================

def sign_data(data):

    return hmac.new(
        SHARED_SECRET,
        data,
        hashlib.sha256
    ).hexdigest()


def verify_signature(data, signature):

    if not signature:
        return False

    expected = sign_data(data)

    return hmac.compare_digest(
        expected,
        signature
    )


# ===============================================================
# TASK FUNCTIONS
# ===============================================================

def add(a, b):

    return a + b


def multiply(a, b):

    return a * b


def divide(a, b):

    if b == 0:
        raise ValueError(
            "Cannot divide by zero"
        )

    return a / b


def complex_math(number):

    total = 0

    for i in range(1, number + 1):

        total += i * i

    return total


# ===============================================================
# PEER CLASS
# ===============================================================

class Peer:

    def __init__(
        self,
        node_id,
        host,
        port
    ):

        self.node_id = node_id

        self.host = host

        self.port = port

        self.cpu = 100.0

        self.memory = 100.0

        self.last_seen = current_time()


    def update_status(
        self,
        cpu,
        memory
    ):

        self.cpu = cpu

        self.memory = memory

        self.last_seen = current_time()


    def mark_alive(self):

        self.last_seen = current_time()


    def is_alive(self):

        return (
            current_time() - self.last_seen
            <= PEER_TIMEOUT
        )


# ===============================================================
# UDP PROTOCOL
# ===============================================================

class MeshProtocol(
    asyncio.DatagramProtocol
):

    def __init__(self, node):

        self.node = node

        self.transport = None


    def connection_made(
        self,
        transport
    ):

        self.transport = transport

        self.node.transport = transport

        print(
            GREEN +
            f"[NETWORK] Node {self.node.node_id} "
            f"started on UDP port {self.node.port}" +
            RESET
        )


    def datagram_received(
        self,
        data,
        address
    ):

        asyncio.create_task(
            self.node.handle_message(
                data,
                address
            )
        )


    def error_received(
        self,
        error
    ):

        print(
            RED +
            f"[NETWORK ERROR] {error}" +
            RESET
        )


# ===============================================================
# MESH NODE
# ===============================================================

class MeshNode:

    def __init__(
        self,
        node_id,
        port,
        bootstrap_port=None
    ):

        self.node_id = node_id

        self.port = port

        self.bootstrap_port = bootstrap_port

        self.transport = None

        self.peers = {}

        self.pending_tasks = {}

        self.running = True


    # ===========================================================
    # START
    # ===========================================================

    async def start(self):

        loop = asyncio.get_running_loop()


        transport, protocol = (
            await loop.create_datagram_endpoint(

                lambda:
                    MeshProtocol(self),

                local_addr=(
                    "127.0.0.1",
                    self.port
                )
            )
        )


        self.transport = transport


        print()

        print(
            CYAN +
            "=" * 65 +
            RESET
        )

        print(
            BOLD +
            "                    MESHWEAVER" +
            RESET
        )

        print(
            "           Zero-Dependency P2P Async Task Broker"
        )

        print(
            CYAN +
            "=" * 65 +
            RESET
        )

        print(
            f"Node ID : {self.node_id}"
        )

        print(
            f"Port    : {self.port}"
        )

        print(
            CYAN +
            "=" * 65 +
            RESET
        )

        print()


        # Start background services

        asyncio.create_task(
            self.gossip_loop()
        )

        asyncio.create_task(
            self.heartbeat_loop()
        )

        asyncio.create_task(
            self.dashboard_loop()
        )


        # Connect to bootstrap peer

        if self.bootstrap_port:

            await asyncio.sleep(1)

            self.discover_peer(
                self.bootstrap_port
            )


        try:

            while self.running:

                await asyncio.sleep(1)


        except KeyboardInterrupt:

            pass


        finally:

            self.stop()


    # ===========================================================
    # SEND UDP
    # ===========================================================

    def send_message(
        self,
        message,
        port
    ):

        if not self.transport:

            return


        try:

            data = json.dumps(
                message
            ).encode()


            self.transport.sendto(

                data,

                (
                    "127.0.0.1",
                    port
                )
            )


        except Exception as error:

            print(
                RED +
                f"[SEND ERROR] {error}" +
                RESET
            )


    # ===========================================================
    # DISCOVERY
    # ===========================================================

    def discover_peer(
        self,
        port
    ):

        message = {

            "type": "DISCOVER",

            "node_id": self.node_id,

            "port": self.port
        }


        print(
            BLUE +
            f"[DISCOVERY] Searching for peer "
            f"at port {port}" +
            RESET
        )


        self.send_message(
            message,
            port
        )


    # ===========================================================
    # ADD PEER
    # ===========================================================

    def add_peer(
        self,
        node_id,
        host,
        port
    ):

        if not node_id:

            return


        if node_id == self.node_id:

            return


        if node_id not in self.peers:

            self.peers[node_id] = Peer(

                node_id,

                host,

                int(port)
            )


            print(
                GREEN +
                f"[DHT] Added peer "
                f"{node_id}:{port}" +
                RESET
            )


        else:

            self.peers[
                node_id
            ].host = host

            self.peers[
                node_id
            ].port = int(port)

            self.peers[
                node_id
            ].mark_alive()


    # ===========================================================
    # MESSAGE HANDLER
    # ===========================================================

    async def handle_message(
        self,
        data,
        address
    ):

        try:

            message = json.loads(
                data.decode()
            )


        except Exception:

            print(
                YELLOW +
                "[NETWORK] Invalid message received." +
                RESET
            )

            return


        message_type = message.get(
            "type"
        )


        # -------------------------------------------------------
        # DISCOVERY
        # -------------------------------------------------------

        if message_type == "DISCOVER":

            await self.handle_discovery(
                message,
                address
            )


        # -------------------------------------------------------
        # PEER LIST
        # -------------------------------------------------------

        elif message_type == "PEER_LIST":

            self.handle_peer_list(
                message
            )


        # -------------------------------------------------------
        # GOSSIP
        # -------------------------------------------------------

        elif message_type == "GOSSIP":

            self.handle_gossip(
                message
            )


        # -------------------------------------------------------
        # HEARTBEAT
        # -------------------------------------------------------

        elif message_type == "HEARTBEAT":

            self.handle_heartbeat(
                message,
                address
            )


        # -------------------------------------------------------
        # HEARTBEAT ACK
        # -------------------------------------------------------

        elif message_type == "HEARTBEAT_ACK":

            self.handle_heartbeat_ack(
                message
            )


        # -------------------------------------------------------
        # TASK
        # -------------------------------------------------------

        elif message_type == "TASK":

            await self.handle_task(
                message,
                address
            )


        # -------------------------------------------------------
        # TASK ACK
        # -------------------------------------------------------

        elif message_type == "TASK_ACK":

            self.handle_task_ack(
                message
            )


    # ===========================================================
    # DISCOVERY HANDLER
    # ===========================================================

    async def handle_discovery(
        self,
        message,
        address
    ):

        peer_id = message.get(
            "node_id"
        )

        peer_port = message.get(
            "port"
        )


        if peer_id == self.node_id:

            return


        self.add_peer(

            peer_id,

            address[0],

            peer_port
        )


        print(
            GREEN +
            f"[DISCOVERY] Node {self.node_id} "
            f"found Node {peer_id}" +
            RESET
        )


        # Build peer list

        peer_list = []


        for peer in self.peers.values():

            peer_list.append({

                "node_id":
                    peer.node_id,

                "host":
                    peer.host,

                "port":
                    peer.port,

                "cpu":
                    peer.cpu,

                "memory":
                    peer.memory
            })


        # Add our own information

        status = get_system_status()


        peer_list.append({

            "node_id":
                self.node_id,

            "host":
                "127.0.0.1",

            "port":
                self.port,

            "cpu":
                status["cpu"],

            "memory":
                status["memory"]
        })


        response = {

            "type":
                "PEER_LIST",

            "node_id":
                self.node_id,

            "peers":
                peer_list
        }


        self.send_message(

            response,

            int(peer_port)
        )


    # ===========================================================
    # PEER LIST HANDLER
    # ===========================================================

    def handle_peer_list(
        self,
        message
    ):

        peers = message.get(
            "peers",
            []
        )


        for peer_data in peers:

            node_id = peer_data.get(
                "node_id"
            )

            port = peer_data.get(
                "port"
            )


            if not node_id or not port:

                continue


            if node_id == self.node_id:

                continue


            self.add_peer(

                node_id,

                peer_data.get(
                    "host",
                    "127.0.0.1"
                ),

                port
            )


            # Load CPU/RAM if available

            if node_id in self.peers:

                self.peers[
                    node_id
                ].cpu = peer_data.get(
                    "cpu",
                    100.0
                )

                self.peers[
                    node_id
                ].memory = peer_data.get(
                    "memory",
                    100.0
                )


        print(
            BLUE +
            f"[DHT] Routing table contains "
            f"{len(self.peers)} peer(s)." +
            RESET
        )


    # ===========================================================
    # GOSSIP LOOP
    # ===========================================================

    async def gossip_loop(self):

        while self.running:

            status = get_system_status()


            message = {

                "type":
                    "GOSSIP",

                "node_id":
                    self.node_id,

                "port":
                    self.port,

                "cpu":
                    status["cpu"],

                "memory":
                    status["memory"]
            }


            for peer in list(
                self.peers.values()
            ):

                self.send_message(

                    message,

                    peer.port
                )


            await asyncio.sleep(
                GOSSIP_INTERVAL
            )


    # ===========================================================
    # GOSSIP HANDLER
    # ===========================================================

    def handle_gossip(
        self,
        message
    ):

        node_id = message.get(
            "node_id"
        )


        port = message.get(
            "port"
        )


        cpu = message.get(
            "cpu",
            100.0
        )


        memory = message.get(
            "memory",
            100.0
        )


        if node_id == self.node_id:

            return


        self.add_peer(

            node_id,

            "127.0.0.1",

            port
        )


        peer = self.peers[
            node_id
        ]


        peer.update_status(

            cpu,

            memory
        )


        print(
            BLUE +
            f"[GOSSIP] Node {node_id} "
            f"CPU={cpu:.1f}% "
            f"RAM={memory:.1f}%" +
            RESET
        )


    # ===========================================================
    # HEARTBEAT LOOP
    # ===========================================================

    async def heartbeat_loop(self):

        while self.running:

            # Send heartbeat

            for peer in list(
                self.peers.values()
            ):

                self.send_message(

                    {
                        "type":
                            "HEARTBEAT",

                        "node_id":
                            self.node_id,

                        "port":
                            self.port
                    },

                    peer.port
                )


            # Detect dead nodes

            dead_nodes = []


            for node_id, peer in list(
                self.peers.items()
            ):

                if not peer.is_alive():

                    dead_nodes.append(
                        node_id
                    )


            for node_id in dead_nodes:

                print(
                    RED +
                    f"[FAULT] Node {node_id} "
                    f"has gone offline." +
                    RESET
                )


                del self.peers[
                    node_id
                ]


            await asyncio.sleep(
                HEARTBEAT_INTERVAL
            )


    # ===========================================================
    # HEARTBEAT HANDLER
    # ===========================================================

    def handle_heartbeat(
        self,
        message,
        address
    ):

        node_id = message.get(
            "node_id"
        )

        port = message.get(
            "port"
        )


        if node_id == self.node_id:

            return


        self.add_peer(

            node_id,

            address[0],

            port
        )


        self.peers[
            node_id
        ].mark_alive()


        self.send_message(

            {
                "type":
                    "HEARTBEAT_ACK",

                "node_id":
                    self.node_id,

                "port":
                    self.port
            },

            int(port)
        )


    # ===========================================================
    # HEARTBEAT ACK
    # ===========================================================

    def handle_heartbeat_ack(
        self,
        message
    ):

        node_id = message.get(
            "node_id"
        )


        if node_id in self.peers:

            self.peers[
                node_id
            ].mark_alive()


    # ===========================================================
    # SELECT LOWEST CPU WORKER
    # ===========================================================

    def select_best_worker(
        self,
        excluded=None
    ):

        if excluded is None:

            excluded = set()


        healthy_workers = [

            peer

            for peer in self.peers.values()

            if (
                peer.node_id
                not in excluded
                and peer.is_alive()
            )
        ]


        if not healthy_workers:

            return None


        return min(

            healthy_workers,

            key=lambda peer: (
                peer.cpu,
                peer.memory
            )
        )


    # ===========================================================
    # CREATE TASK
    # ===========================================================

    def create_task(
        self,
        reply_port
    ):

        task_id = create_task_id()


        # This is the function that will execute remotely.

        task_package = {

            "task_id":
                task_id,

            "task":
                multiply,

            "args":
                (
                    6,
                    7
                )
        }


        serialized = cloudpickle.dumps(
            task_package
        )


        encoded = base64.b64encode(
            serialized
        ).decode()


        signature = sign_data(
            serialized
        )


        return {

            "type":
                "TASK",

            "task_id":
                task_id,

            "payload":
                encoded,

            "signature":
                signature,

            "source":
                "CLIENT",

            "reply_host":
                "127.0.0.1",

            "reply_port":
                reply_port,

            "execute_here":
                False
        }


    # ===========================================================
    # HANDLE INCOMING TASK
    # ===========================================================

    async def handle_task(
        self,
        message,
        address
    ):

        execute_here = message.get(
            "execute_here",
            False
        )


        # =======================================================
        # ROUTER MODE
        # =======================================================

        if not execute_here:

            await self.route_task(
                message
            )

            return


        # =======================================================
        # WORKER MODE
        # =======================================================

        await self.execute_task(
            message
        )


    # ===========================================================
    # ROUTE TASK
    # ===========================================================

    async def route_task(
        self,
        task_message
    ):

        task_id = task_message.get(
            "task_id"
        )


        tried_nodes = set()


        while True:

            worker = self.select_best_worker(
                tried_nodes
            )


            if worker is None:

                print(
                    RED +
                    "[ROUTER] No healthy worker available." +
                    RESET
                )


                self.send_task_failure(
                    task_message,
                    "No healthy worker available"
                )


                return


            tried_nodes.add(
                worker.node_id
            )


            print()

            print(
                CYAN +
                "=" * 60 +
                RESET
            )

            print(
                BOLD +
                "                  TASK ROUTING" +
                RESET
            )

            print(
                f"Task ID       : {task_id}"
            )

            print(
                f"Selected Node : {worker.node_id}"
            )

            print(
                f"CPU Usage     : {worker.cpu:.1f}%"
            )

            print(
                f"RAM Usage     : {worker.memory:.1f}%"
            )

            print(
                f"Port          : {worker.port}"
            )

            print(
                CYAN +
                "=" * 60 +
                RESET
            )


            # Make a copy so the original task
            # can still be rerouted.

            routed_task = dict(
                task_message
            )


            routed_task[
                "execute_here"
            ] = True


            routed_task[
                "router_id"
            ] = self.node_id


            # Send task to worker

            self.send_message(

                routed_task,

                worker.port
            )


            print(
                GREEN +
                f"[ROUTER] Task {task_id} "
                f"sent to Node {worker.node_id}" +
                RESET
            )


            # Wait for worker ACK

            ack_received = False

            start_time = current_time()


            while (
                current_time() - start_time
                < TASK_TIMEOUT
            ):

                task_state = self.pending_tasks.get(
                    task_id
                )


                if (
                    task_state
                    and task_state.get(
                        "ack"
                    )
                ):

                    ack_received = True

                    break


                await asyncio.sleep(
                    0.1
                )


            if ack_received:

                print(
                    GREEN +
                    f"[ROUTER] Node {worker.node_id} "
                    f"accepted task {task_id}." +
                    RESET
                )


                return


            # Worker did not respond

            print(
                RED +
                f"[FAULT] Node {worker.node_id} "
                f"did not acknowledge task." +
                RESET
            )


            print(
                YELLOW +
                "[ROUTER] Re-routing task..." +
                RESET
            )


            # Remove failed worker

            if worker.node_id in self.peers:

                del self.peers[
                    worker.node_id
                ]


            # Create new state

            self.pending_tasks[
                task_id
            ] = {

                "ack":
                    False,

                "worker":
                    None
            }


            # If all nodes were tried

            if len(tried_nodes) >= (
                len(self.peers) + 1
            ):

                print(
                    RED +
                    "[ROUTER] All workers failed." +
                    RESET
                )


                self.send_task_failure(
                    task_message,
                    "All worker nodes failed"
                )


                return


    # ===========================================================
    # EXECUTE TASK
    # ===========================================================

    async def execute_task(
        self,
        message
    ):

        task_id = message.get(
            "task_id"
        )


        payload = message.get(
            "payload"
        )


        signature = message.get(
            "signature"
        )


        router_id = message.get(
            "router_id"
        )


        reply_port = message.get(
            "reply_port"
        )


        # -------------------------------------------------------
        # Validate task
        # -------------------------------------------------------

        try:

            serialized = base64.b64decode(
                payload
            )


        except Exception:

            print(
                RED +
                "[SECURITY] Invalid task payload." +
                RESET
            )

            return


        # -------------------------------------------------------
        # Verify signature
        # -------------------------------------------------------

        if not verify_signature(
            serialized,
            signature
        ):

            print(
                RED +
                "[SECURITY] Task signature verification failed!" +
                RESET
            )

            return


        try:

            task_package = cloudpickle.loads(
                serialized
            )


            task = task_package[
                "task"
            ]


            args = task_package.get(
                "args",
                ()
            )


        except Exception as error:

            print(
                RED +
                f"[EXECUTOR] Invalid task: {error}" +
                RESET
            )

            return


        # -------------------------------------------------------
        # Send ACK to router BEFORE execution
        # -------------------------------------------------------

        if router_id in self.peers:

            router_port = self.peers[
                router_id
            ].port


            self.send_message(

                {
                    "type":
                        "TASK_ACK",

                    "task_id":
                        task_id,

                    "worker":
                        self.node_id
                },

                router_port
            )


        print()

        print(
            GREEN +
            "=" * 60 +
            RESET
        )

        print(
            BOLD +
            "                  TASK EXECUTION" +
            RESET
        )

        print(
            f"Node       : {self.node_id}"
        )

        print(
            f"Task ID    : {task_id}"
        )

        print(
            f"Function   : {task.__name__}"
        )

        print(
            f"Arguments  : {args}"
        )

        print(
            GREEN +
            "=" * 60 +
            RESET
        )


        try:

            result = task(
                *args
            )


            print(
                GREEN +
                f"[EXECUTOR] Task completed."
                f" Result = {result}" +
                RESET
            )


            result_message = {

                "type":
                    "TASK_RESULT",

                "task_id":
                    task_id,

                "worker":
                    self.node_id,

                "success":
                    True,

                "result":
                    result
            }


        except Exception as error:

            print(
                RED +
                f"[EXECUTOR ERROR] {error}" +
                RESET
            )


            result_message = {

                "type":
                    "TASK_RESULT",

                "task_id":
                    task_id,

                "worker":
                    self.node_id,

                "success":
                    False,

                "error":
                    str(error)
            }


        # -------------------------------------------------------
        # Send result directly to original sender
        # -------------------------------------------------------

        if reply_port:

            self.send_message(

                result_message,

                int(reply_port)
            )


    # ===========================================================
    # TASK ACK
    # ===========================================================

    def handle_task_ack(
        self,
        message
    ):

        task_id = message.get(
            "task_id"
        )


        worker = message.get(
            "worker"
        )


        if task_id not in self.pending_tasks:

            self.pending_tasks[
                task_id
            ] = {}


        self.pending_tasks[
            task_id
        ][
            "ack"
        ] = True


        self.pending_tasks[
            task_id
        ][
            "worker"
        ] = worker


        print(
            GREEN +
            f"[ACK] Node {worker} "
            f"accepted task {task_id}." +
            RESET
        )


    # ===========================================================
    # TASK FAILURE
    # ===========================================================

    def send_task_failure(
        self,
        task_message,
        reason
    ):

        reply_port = task_message.get(
            "reply_port"
        )


        if not reply_port:

            return


        message = {

            "type":
                "TASK_RESULT",

            "task_id":
                task_message.get(
                    "task_id"
                ),

            "worker":
                self.node_id,

            "success":
                False,

            "error":
                reason
        }


        self.send_message(

            message,

            int(reply_port)
        )


    # ===========================================================
    # DASHBOARD
    # ===========================================================

    async def dashboard_loop(self):

        while self.running:

            await asyncio.sleep(
                15
            )


            self.show_dashboard()


    # ===========================================================
    # SHOW DASHBOARD
    # ===========================================================

    def show_dashboard(self):

        status = get_system_status()


        print()

        print(
            CYAN +
            "=" * 70 +
            RESET
        )

        print(
            BOLD +
            "                     MESH STATUS" +
            RESET
        )

        print(
            CYAN +
            "=" * 70 +
            RESET
        )


        print(
            f"{'NODE':<12}"
            f"{'PORT':<10}"
            f"{'CPU':<12}"
            f"{'RAM':<12}"
            f"{'STATUS':<12}"
        )


        print(
            "-" * 70
        )


        print(
            f"{self.node_id:<12}"
            f"{self.port:<10}"
            f"{status['cpu']:<12.1f}"
            f"{status['memory']:<12.1f}"
            f"{GREEN}ONLINE{RESET}"
        )


        for peer in self.peers.values():

            if peer.is_alive():

                status_text = "ONLINE"

            else:

                status_text = "OFFLINE"


            print(
                f"{peer.node_id:<12}"
                f"{peer.port:<10}"
                f"{peer.cpu:<12.1f}"
                f"{peer.memory:<12.1f}"
                f"{GREEN if status_text == 'ONLINE' else RED}"
                f"{status_text}"
                f"{RESET}"
            )


        print(
            CYAN +
            "=" * 70 +
            RESET
        )


    # ===========================================================
    # STOP
    # ===========================================================

    def stop(self):

        self.running = False


        if self.transport:

            self.transport.close()


        print()

        print(
            YELLOW +
            f"[MESH] Node {self.node_id} stopped." +
            RESET
        )


# ===============================================================
# TASK SENDER
# ===============================================================

async def send_task(
    router_port
):

    loop = asyncio.get_running_loop()


    class SenderProtocol(
        asyncio.DatagramProtocol
    ):

        def __init__(self):

            self.transport = None

            self.done = (
                loop.create_future()
            )


        def connection_made(
            self,
            transport
        ):

            self.transport = transport


            # Get temporary UDP port

            local_address = (
                transport.get_extra_info(
                    "sockname"
                )
            )


            local_port = local_address[1]


            task_id = create_task_id()


            # ---------------------------------------------------
            # Create task
            # ---------------------------------------------------

            task_package = {

                "task_id":
                    task_id,

                "task":
                    multiply,

                "args":
                    (
                        6,
                        7
                    )
            }


            serialized = cloudpickle.dumps(
                task_package
            )


            encoded = base64.b64encode(
                serialized
            ).decode()


            signature = sign_data(
                serialized
            )


            message = {

                "type":
                    "TASK",

                "task_id":
                    task_id,

                "payload":
                    encoded,

                "signature":
                    signature,

                "source":
                    "CLIENT",

                "reply_host":
                    "127.0.0.1",

                "reply_port":
                    local_port,

                "execute_here":
                    False
            }


            data = json.dumps(
                message
            ).encode()


            transport.sendto(

                data,

                (
                    "127.0.0.1",
                    router_port
                )
            )


            print()

            print(
                CYAN +
                "=" * 60 +
                RESET
            )

            print(
                BOLD +
                "              MESHWEAVER TASK SENDER" +
                RESET
            )

            print(
                CYAN +
                "=" * 60 +
                RESET
            )

            print(
                f"Task ID  : {task_id}"
            )

            print(
                f"Function : multiply(6, 7)"
            )

            print(
                f"Router   : port {router_port}"
            )

            print(
                f"Reply    : port {local_port}"
            )

            print(
                "Waiting for result..."
            )

            print()


        def datagram_received(
            self,
            data,
            address
        ):

            try:

                message = json.loads(
                    data.decode()
                )


                if message.get(
                    "type"
                ) != "TASK_RESULT":

                    return


                task_id = message.get(
                    "task_id"
                )


                print()

                if message.get(
                    "success"
                ):

                    print(
                        GREEN +
                        "╔══════════════════════════════════════╗"
                        + RESET
                    )

                    print(
                        GREEN +
                        "║          TASK COMPLETED              ║"
                        + RESET
                    )

                    print(
                        GREEN +
                        "╚══════════════════════════════════════╝"
                        + RESET
                    )

                    print(
                        f"Task ID : {task_id}"
                    )

                    print(
                        f"Worker  : Node "
                        f"{message.get('worker')}"
                    )

                    print(
                        f"Result  : "
                        f"{message.get('result')}"
                    )


                else:

                    print(
                        RED +
                        "[TASK FAILED]" +
                        RESET
                    )

                    print(
                        f"Reason : "
                        f"{message.get('error')}"
                    )


                if not self.done.done():

                    self.done.set_result(
                        message
                    )


            except Exception as error:

                print(
                    RED +
                    f"[CLIENT ERROR] {error}" +
                    RESET
                )


        def error_received(
            self,
            error
        ):

            print(
                RED +
                f"[CLIENT NETWORK ERROR] {error}" +
                RESET
            )


    transport, protocol = (

        await loop.create_datagram_endpoint(

            lambda:
                SenderProtocol(),

            local_addr=(
                "127.0.0.1",
                0
            )
        )
    )


    try:

        await asyncio.wait_for(

            protocol.done,

            timeout=20
        )


    except asyncio.TimeoutError:

        print()

        print(
            RED +
            "[CLIENT] Timed out waiting for result." +
            RESET
        )


    finally:

        transport.close()


# ===============================================================
# MAIN
# ===============================================================

def main():

    parser = argparse.ArgumentParser(

        description=(
            "MeshWeaver - "
            "Zero-Dependency P2P Async Task Broker"
        )
    )


    subparsers = parser.add_subparsers(
        dest="command"
    )


    # ===========================================================
    # NODE
    # ===========================================================

    node_parser = subparsers.add_parser(
        "node",
        help="Start a MeshWeaver node"
    )


    node_parser.add_argument(
        "node_id",
        help="Node ID"
    )


    node_parser.add_argument(
        "port",
        type=int,
        help="UDP port"
    )


    node_parser.add_argument(
        "--peer",
        type=int,
        help="Bootstrap peer port"
    )


    # ===========================================================
    # SEND
    # ===========================================================

    send_parser = subparsers.add_parser(
        "send",
        help="Send a test task"
    )


    send_parser.add_argument(
        "port",
        type=int,
        help="Router node port"
    )


    # ===========================================================
    # RUN
    # ===========================================================

    args = parser.parse_args()


    # ===========================================================
    # START NODE
    # ===========================================================

    if args.command == "node":

        node = MeshNode(

            args.node_id,

            args.port,

            args.peer
        )


        try:

            asyncio.run(
                node.start()
            )

        except KeyboardInterrupt:

            pass


    # ===========================================================
    # SEND TASK
    # ===========================================================

    elif args.command == "send":

        asyncio.run(
            send_task(
                args.port
            )
        )


    # ===========================================================
    # NO COMMAND
    # ===========================================================

    else:

        parser.print_help()


# ===============================================================
# ENTRY POINT
# ===============================================================

if __name__ == "__main__":

    main()