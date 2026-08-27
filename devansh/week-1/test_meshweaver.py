import asyncio
import unittest
from meshweaver.protocol import Message, MessageType, MAGIC_BYTES
from meshweaver.serializer import TaskSerializer
from meshweaver.executor import TaskExecutor
from meshweaver.node import MeshNode


def sample_add(a: int, b: int) -> int:
    return a + b


def sample_raising_func():
    raise ValueError("Test error inside remote function")


class TestMeshWeaverProtocol(unittest.TestCase):
    def test_message_encoding_decoding(self):
        msg = Message(
            msg_type=MessageType.PING,
            sender_id="node-test-1",
            payload=b"hello meshweaver",
        )
        encoded = msg.encode()
        decoded = Message.decode(encoded)

        self.assertEqual(decoded.msg_type, MessageType.PING)
        self.assertEqual(decoded.sender_id, "node-test-1")
        self.assertEqual(decoded.payload, b"hello meshweaver")
        self.assertEqual(decoded.msg_id, msg.msg_id)

    def test_invalid_magic_bytes(self):
        msg = Message(msg_type=MessageType.PONG, sender_id="node-1", payload=b"")
        encoded = bytearray(msg.encode())
        encoded[0:4] = b"XXXX"
        with self.assertRaises(ValueError):
            Message.decode(bytes(encoded))


class TestTaskSerializationAndExecution(unittest.IsolatedAsyncioTestCase):
    async def test_serialize_and_execute_function(self):
        payload_bytes = TaskSerializer.serialize_task(sample_add, (15, 27), {})
        executor = TaskExecutor()
        task_result = await executor.execute_task("task-123", payload_bytes)

        self.assertTrue(task_result.success)
        self.assertEqual(task_result.task_id, "task-123")
        
        result_val = TaskSerializer.deserialize_result(task_result.result_bytes)
        self.assertEqual(result_val, 42)

    async def test_serialize_lambda(self):
        multiply_lambda = lambda x, y: x * y
        payload_bytes = TaskSerializer.serialize_task(multiply_lambda, (6, 7), {})
        executor = TaskExecutor()
        task_result = await executor.execute_task("task-lambda", payload_bytes)

        self.assertTrue(task_result.success)
        result_val = TaskSerializer.deserialize_result(task_result.result_bytes)
        self.assertEqual(result_val, 42)

    async def test_execution_failure_handling(self):
        payload_bytes = TaskSerializer.serialize_task(sample_raising_func, (), {})
        executor = TaskExecutor()
        task_result = await executor.execute_task("task-err", payload_bytes)

        self.assertFalse(task_result.success)
        self.assertIn("ValueError", task_result.error)


class TestMeshNodeIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.node_a = MeshNode(host="127.0.0.1", port=0, node_id="Node-Alpha")
        self.node_b = MeshNode(host="127.0.0.1", port=0, node_id="Node-Beta")
        await self.node_a.start()
        await self.node_b.start()

    async def asyncTearDown(self):
        await self.node_a.stop()
        await self.node_b.stop()

    async def test_p2p_ping_pong(self):
        rtt = await self.node_a.ping(self.node_b.host, self.node_b.port, timeout=2.0)
        self.assertGreaterEqual(rtt, 0.0)
        self.assertIn("Node-Alpha", self.node_b.peers)

    async def test_remote_task_execution(self):
        res = await self.node_a.submit_task(
            self.node_b.host,
            self.node_b.port,
            sample_add,
            100,
            250,
            timeout=5.0,
        )
        self.assertEqual(res, 350)

    async def test_remote_closure_task_execution(self):
        factor = 10
        def compute_with_closure(arr):
            return [x * factor for x in arr]

        res = await self.node_a.submit_task(
            self.node_b.host,
            self.node_b.port,
            compute_with_closure,
            [1, 2, 3, 4],
            timeout=5.0,
        )
        self.assertEqual(res, [10, 20, 30, 40])


if __name__ == "__main__":
    unittest.main()
