import asyncio
import socket
from dht_node import DHTNode


# ==========================================
# Configuration
# ==========================================

HOST = "127.0.0.1"

# 10 nodes
PORTS = [
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
# Start one node
# ==========================================

async def start_node(port, bootstrap_port=None):

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: DHTNode(port),
        local_addr=(HOST, port)
    )

    print(f"[STARTED] Node {port}")

    # Give the node time to start
    await asyncio.sleep(0.5)

    # Join bootstrap node
    if bootstrap_port is not None:

        protocol.join_peer(
            HOST,
            bootstrap_port
        )

        print(
            f"[JOIN] Node {port} -> "
            f"{HOST}:{bootstrap_port}"
        )

    return transport, protocol


# ==========================================
# Audit
# ==========================================

async def audit():

    print("\n")
    print("=" * 60)
    print("           10-NODE DHT NETWORK AUDIT")
    print("=" * 60)
    print()

    nodes = []

    # --------------------------------------
    # Start bootstrap node
    # --------------------------------------

    transport, protocol = await start_node(
        BOOTSTRAP_PORT
    )

    nodes.append(
        (BOOTSTRAP_PORT, transport, protocol)
    )

    await asyncio.sleep(1)

    # --------------------------------------
    # Start remaining 9 nodes
    # --------------------------------------

    for port in PORTS[1:]:

        transport, protocol = await start_node(
            port,
            BOOTSTRAP_PORT
        )

        nodes.append(
            (port, transport, protocol)
        )

        await asyncio.sleep(0.5)

    # --------------------------------------
    # Allow JOIN / PEERS traffic to settle
    # --------------------------------------

    print("\n")
    print("=" * 60)
    print("Waiting for peer discovery...")
    print("=" * 60)

    await asyncio.sleep(3)

    # ======================================
    # AUDIT RESULTS
    # ======================================

    print("\n")
    print("=" * 60)
    print("                 AUDIT RESULTS")
    print("=" * 60)

    total_nodes = len(nodes)

    print(f"\nTotal nodes started : {total_nodes}")

    # --------------------------------------
    # Node information
    # --------------------------------------

    unique_ids = set()

    print("\nNODE STATUS")
    print("-" * 60)

    for port, transport, protocol in nodes:

        node_id = protocol.node_id

        unique_ids.add(node_id)

        peer_count = len(protocol.peers)

        stats_count = len(protocol.peer_stats)

        print(
            f"Node {port}: "
            f"ID={node_id[:12]}... "
            f"Peers={peer_count} "
            f"Stats={stats_count}"
        )

    # --------------------------------------
    # Unique Node ID test
    # --------------------------------------

    print("\n")
    print("NODE ID TEST")
    print("-" * 60)

    if len(unique_ids) == total_nodes:

        print("[PASS] All node IDs are unique")

    else:

        print(
            "[FAIL] Duplicate node IDs detected"
        )

    # --------------------------------------
    # Peer discovery test
    # --------------------------------------

    print("\n")
    print("PEER DISCOVERY TEST")
    print("-" * 60)

    discovery_pass = True

    for port, transport, protocol in nodes:

        # Bootstrap itself has no bootstrap peer
        if port == BOOTSTRAP_PORT:
            continue

        peer_count = len(protocol.peers)

        if peer_count >= 1:

            print(
                f"[PASS] Node {port} "
                f"knows {peer_count} peer(s)"
            )

        else:

            print(
                f"[FAIL] Node {port} "
                f"knows no peers"
            )

            discovery_pass = False

    # --------------------------------------
    # Closest peer test
    # --------------------------------------

    print("\n")
    print("KAD CLOSEST-PEER TEST")
    print("-" * 60)

    closest_pass = True

    for port, transport, protocol in nodes:

        closest = protocol.get_closest_peers(3)

        if closest:

            print(
                f"[PASS] Node {port}: "
                f"{len(closest)} closest peer(s)"
            )

        else:

            # Bootstrap node may have peers after joins
            print(
                f"[WARN] Node {port}: "
                f"no closest peers"
            )

            closest_pass = False

    # --------------------------------------
    # UDP PING TEST
    # --------------------------------------

    print("\n")
    print("UDP PING TEST")
    print("-" * 60)

    # Send PING from every node to every known peer
    for port, transport, protocol in nodes:

        for peer_id, address in protocol.peers.items():

            protocol.ping_peer(address)

    print(
        "[INFO] PING packets sent to known peers"
    )

    # Give PONG packets time to arrive
    await asyncio.sleep(2)

    # --------------------------------------
    # Final summary
    # --------------------------------------

    print("\n")
    print("=" * 60)
    print("                 FINAL SUMMARY")
    print("=" * 60)

    print(
        f"\nNodes started      : {total_nodes}"
    )

    print(
        f"Unique IDs         : "
        f"{len(unique_ids)} / {total_nodes}"
    )

    print(
        "Peer discovery     : "
        + ("PASS" if discovery_pass else "FAIL")
    )

    print(
        "Closest peer logic : "
        + ("PASS" if closest_pass else "WARN")
    )

    print(
        "UDP communication  : CHECK PONG OUTPUT"
    )

    print("\n")
    print("=" * 60)
    print("Audit complete.")
    print("=" * 60)

    # ======================================
    # Keep network alive for gossip test
    # ======================================

    print(
        "\nKeeping nodes alive for 15 seconds "
        "to observe gossip..."
    )

    await asyncio.sleep(15)

    # ======================================
    # Shutdown
    # ======================================

    print("\n")
    print("=" * 60)
    print("Shutting down nodes...")
    print("=" * 60)

    for port, transport, protocol in nodes:

        transport.close()

        print(
            f"[STOPPED] Node {port}"
        )


# ==========================================
# Main
# ==========================================

if __name__ == "__main__":

    try:

        asyncio.run(audit())

    except KeyboardInterrupt:

        print(
            "\nAudit interrupted by user."
        )

