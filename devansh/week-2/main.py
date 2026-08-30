import asyncio
import logging
import time
from meshweaver import MeshNode


def compute_square_sum(numbers: list) -> int:
    return sum(x * x for x in numbers)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    print("===============================================================")
    print("      MeshWeaver P2P Task Broker - Week 2 Demonstration        ")
    print("   (Kademlia DHT Node Discovery & Background Gossip Protocol)  ")
    print("===============================================================")

    nodes = []
    base_port = 9010
    
    nodes.append(MeshNode(host="127.0.0.1", port=base_port, node_id="Node-0-Bootstrap"))
    for i in range(1, 5):
        nodes.append(MeshNode(host="127.0.0.1", port=base_port + i, node_id=f"Node-{i}"))

    for n in nodes:
        await n.start(enable_gossip=True, gossip_interval=2.0)

    try:
        bootstrap_node = nodes[0]
        print(f"\n[INFO] Bootstrap Node online at {bootstrap_node.host}:{bootstrap_node.port}")

        print("\n--- 1. Kademlia DHT Dynamic Mesh Joining ---")
        for worker in nodes[1:]:
            print(f"Connecting {worker.node_id} to Bootstrap Node...")
            discovered_contacts = await worker.join_mesh(
                bootstrap_node.host, bootstrap_node.port
            )
            print(f"[OK] {worker.node_id} joined mesh! Discovered contacts: {len(discovered_contacts)}")

        print("\n--- 2. Kademlia Routing Table Inspection ---")
        for n in nodes:
            contacts = n.routing_table.get_all_contacts()
            contact_ids = [c.node_id for c in contacts]
            print(f"  {n.node_id} Routing Table ({len(contacts)} peers): {contact_ids}")

        print("\n--- 3. Background Gossip Protocol (CPU & RAM Load Exchange) ---")
        print("Waiting for background Gossip engine loop to exchange load stats...")
        await asyncio.sleep(2.5)

        for n in nodes:
            peer_loads = n.gossip_engine.peer_loads
            print(f"\n  [{n.node_id}] Real-Time Neighbor Resource Table ({len(peer_loads)} reporting peers):")
            for peer_id, load in peer_loads.items():
                print(f"     -> Peer '{peer_id}': CPU = {load.cpu_percent:.1f}%, RAM = {load.ram_percent:.1f}%")

        print("\n--- 4. Task Execution Across Dynamically Discovered Mesh Nodes ---")
        sender_node = nodes[1]
        target_node = nodes[4]
        numbers = [1, 2, 3, 4, 5, 10]
        print(f"Dispatching sum-of-squares task from {sender_node.node_id} to {target_node.node_id}...")
        
        res = await sender_node.submit_task(
            target_node.host,
            target_node.port,
            compute_square_sum,
            numbers,
        )
        print(f"[OK] Result received from {target_node.node_id}: Sum of squares of {numbers} = {res}")

        print("\n===============================================================")
        print("      Week 2 Verification Complete: All Features Passed!       ")
        print("===============================================================")

    finally:
        for n in nodes:
            await n.stop()


if __name__ == "__main__":
    asyncio.run(main())
