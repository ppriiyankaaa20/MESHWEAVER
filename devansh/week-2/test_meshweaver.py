import asyncio
import unittest
from meshweaver.routing import Contact, KBucket, RoutingTable, node_id_to_int
from meshweaver.gossip import GossipEngine, NodeLoadState
from meshweaver.node import MeshNode


class TestKademliaRouting(unittest.TestCase):
    def test_node_id_to_int(self):
        val1 = node_id_to_int("node-alpha")
        val2 = node_id_to_int("node-alpha")
        val3 = node_id_to_int("node-beta")
        
        self.assertEqual(val1, val2)
        self.assertNotEqual(val1, val3)

    def test_routing_table_xor_distance(self):
        rt = RoutingTable(node_id="node-main", k=8)
        
        c1 = Contact(node_id="node-near-1", node_id_int=node_id_to_int("node-near-1"), host="127.0.0.1", port=9001)
        c2 = Contact(node_id="node-near-2", node_id_int=node_id_to_int("node-near-2"), host="127.0.0.1", port=9002)
        
        rt.add_contact(c1)
        rt.add_contact(c2)
        
        closest = rt.find_closest_nodes(node_id_to_int("node-main"), count=5)
        self.assertEqual(len(closest), 2)


class TestGossipEngine(unittest.TestCase):
    def test_local_load_generation(self):
        engine = GossipEngine(node_id="test-node")
        load = engine.get_local_load()
        
        self.assertEqual(load.node_id, "test-node")
        self.assertGreaterEqual(load.cpu_percent, 0.0)
        self.assertGreaterEqual(load.ram_percent, 0.0)

    def test_peer_load_update(self):
        engine = GossipEngine(node_id="test-node")
        peer_load = NodeLoadState(node_id="peer-1", cpu_percent=25.5, ram_percent=60.0, timestamp=100.0)
        engine.update_peer_load(peer_load)
        
        self.assertIn("peer-1", engine.peer_loads)
        self.assertEqual(engine.peer_loads["peer-1"].cpu_percent, 25.5)


class TestWeek2Integration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.nodes = [
            MeshNode(host="127.0.0.1", port=0, node_id=f"Node-{i}")
            for i in range(4)
        ]
        for node in self.nodes:
            await node.start(enable_gossip=True, gossip_interval=0.5)

    async def asyncTearDown(self):
        for node in self.nodes:
            await node.stop()

    async def test_kademlia_discovery_and_gossip(self):
        bootstrap = self.nodes[0]
        
        for worker in self.nodes[1:]:
            discovered = await worker.join_mesh(bootstrap.host, bootstrap.port)
            self.assertGreaterEqual(len(discovered), 1)

        await asyncio.sleep(1.2)
        
        for node in self.nodes:
            self.assertGreaterEqual(len(node.gossip_engine.peer_loads), 1)

        res = await self.nodes[1].submit_task(
            self.nodes[2].host,
            self.nodes[2].port,
            lambda x, y: x * y,
            7,
            8,
        )
        self.assertEqual(res, 56)


if __name__ == "__main__":
    unittest.main()
