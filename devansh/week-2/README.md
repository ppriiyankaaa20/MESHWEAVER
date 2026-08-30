# MeshWeaver - Week 2: Kademlia DHT Node Discovery & Gossip Load Protocol

> Zero-Dependency P2P Async Task Broker for Distributed Systems & Edge Computing

---

## Overview

**MeshWeaver** Week 2 introduces **decentralized peer discovery using Kademlia Distributed Hash Table (DHT)** and a **periodic Gossip Protocol** for node load sharing (CPU/RAM metrics).

---

## Architecture & Modules

```
week-2/
├── meshweaver/
│   ├── __init__.py         # Package exports
│   ├── protocol.py         # Binary protocol (FIND_NODE, FIND_NODE_RESPONSE, GOSSIP_LOAD)
│   ├── serializer.py       # Cloudpickle task & result serializer
│   ├── executor.py         # Thread-safe task execution engine
│   ├── routing.py          # Kademlia XOR distance metric & K-Bucket Routing Table
│   ├── gossip.py           # Background Gossip Engine for sharing CPU/RAM load every 5s
│   └── node.py             # MeshNode integrated with Kademlia discovery & Gossip engine
├── main.py                 # Multi-node dynamic mesh discovery and load sharing demo
├── test_meshweaver.py      # Automated test suite for Kademlia & Gossip
└── README.md               # Architecture & execution guide for Week 2
```

### Module Breakdown

1. **`meshweaver.routing`**:
   - `Contact`: Node contact holding `node_id`, `node_id_int` (SHA-1 integer), `host`, `port`.
   - `KBucket`: Stores up to $K=8$ contacts.
   - `RoutingTable`: 160 k-buckets sorted by XOR distance metric $d(A, B) = A \oplus B$.

2. **`meshweaver.gossip`**:
   - `NodeLoadState`: Stores `node_id`, `cpu_percent`, `ram_percent`, `timestamp`.
   - `GossipEngine`: Periodically reads local CPU & RAM usage and broadcasts `GOSSIP_LOAD` messages to neighbors every 5 seconds.

3. **`meshweaver.protocol`**:
   - Extended message types: `FIND_NODE` (`0x05`), `FIND_NODE_RESPONSE` (`0x06`), `GOSSIP_LOAD` (`0x07`).

4. **`meshweaver.node`**:
   - `join_mesh(bootstrap_host, bootstrap_port)`: Dynamic node lookup & mesh joining via Kademlia `FIND_NODE`.

---

## How to Run

### 1. Run Automated Unit & Integration Tests

```bash
cd week-2
python -m unittest test_meshweaver.py
```

### 2. Run the Multi-Node Demonstration Script

```bash
cd week-2
python main.py
```
