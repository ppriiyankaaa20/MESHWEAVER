# MESHWEAVER
Project 2 - "MeshWeaver": Zero-Dependency P2P Async Task Broker
# MeshWeaver - Week 3

## Distributed Task Routing and Fault Tolerance

Week 3 extends the MeshWeaver peer-to-peer network with intelligent task routing and fault tolerance.

### Features

- CPU-based task routing
- Lowest-load peer selection
- Asynchronous UDP communication
- Peer resource monitoring
- Heartbeat-based peer health detection
- Automatic task re-routing
- Remote Python task execution
- Task serialization using cloudpickle

## Technologies

- Python
- asyncio
- UDP sockets
- cloudpickle
- psutil
- Peer-to-peer networking
- Gossip protocol
- Kademlia-style peer discovery

## Architecture

```text
Task Sender
     |
     v
Node A
     |
     v
Task Router
     |
     +----------+
     |          |
     v          v
Node B        Node C
CPU 20%       CPU 60%
     |
     v
Task Executor
     |
     v
Result
     |
     v
Task Sender