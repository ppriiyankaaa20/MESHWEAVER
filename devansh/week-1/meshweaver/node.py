import asyncio
import logging
import time
import uuid
from typing import Any, Callable, Dict, Optional, Tuple

from .protocol import Message, MessageType, TaskResult
from .serializer import TaskSerializer
from .executor import TaskExecutor

logger = logging.getLogger("MeshWeaver.Node")


class MeshNodeProtocol(asyncio.DatagramProtocol):
    def __init__(self, node: "MeshNode"):
        self.node = node

    def connection_made(self, transport: asyncio.DatagramTransport):
        self.node.transport = transport

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        asyncio.create_task(self.node._handle_datagram(data, addr))

    def error_received(self, exc: Exception):
        logger.error(f"[%s] Socket error received: %s", self.node.node_id, exc)

    def connection_lost(self, exc: Optional[Exception]):
        if exc:
            logger.warning(f"[%s] Connection lost with error: %s", self.node.node_id, exc)


class MeshNode:
    def __init__(self, host: str = "127.0.0.1", port: int = 0, node_id: Optional[str] = None):
        self.host = host
        self.port = port
        self.node_id = node_id or f"node-{uuid.uuid4().hex[:8]}"
        
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.protocol: Optional[MeshNodeProtocol] = None
        
        self.peers: Dict[str, Tuple[str, int]] = {}
        self.pending_pings: Dict[str, Tuple[asyncio.Future, float]] = {}
        self.pending_tasks: Dict[str, asyncio.Future] = {}
        
        self.executor = TaskExecutor()
        self._running = False

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: MeshNodeProtocol(self),
            local_addr=(self.host, self.port),
        )
        self.transport = transport
        self.protocol = protocol
        
        sock_addr = transport.get_extra_info("sockname")
        if sock_addr:
            self.host, self.port = sock_addr[0], sock_addr[1]
            
        self._running = True
        logger.info(f"MeshNode [{self.node_id}] started on {self.host}:{self.port}")

    async def stop(self) -> None:
        self._running = False
        if self.transport:
            self.transport.close()
            self.transport = None
            
        for msg_id, (fut, _) in list(self.pending_pings.items()):
            if not fut.done():
                fut.cancel()
        self.pending_pings.clear()
        
        for task_id, fut in list(self.pending_tasks.items()):
            if not fut.done():
                fut.cancel()
        self.pending_tasks.clear()
        
        logger.info(f"MeshNode [{self.node_id}] stopped.")

    async def ping(self, target_host: str, target_port: int, timeout: float = 3.0) -> float:
        if not self.transport:
            raise RuntimeError("Node transport is not started.")

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        
        msg = Message(
            msg_type=MessageType.PING,
            sender_id=self.node_id,
            payload=b"",
        )
        
        send_time = time.perf_counter()
        self.pending_pings[msg.msg_id] = (fut, send_time)
        
        self.transport.sendto(msg.encode(), (target_host, target_port))
        
        try:
            rtt = await asyncio.wait_for(fut, timeout=timeout)
            return rtt
        except asyncio.TimeoutError:
            self.pending_pings.pop(msg.msg_id, None)
            raise TimeoutError(f"Ping to {target_host}:{target_port} timed out after {timeout}s")

    async def submit_task(
        self,
        target_host: str,
        target_port: int,
        func: Callable,
        *args: Any,
        timeout: float = 15.0,
        **kwargs: Any,
    ) -> Any:
        if not self.transport:
            raise RuntimeError("Node transport is not started.")

        loop = asyncio.get_running_loop()
        task_id = str(uuid.uuid4())
        fut = loop.create_future()
        self.pending_tasks[task_id] = fut

        fn_args_bytes = TaskSerializer.serialize_task(func, args, kwargs)
        
        task_id_bytes = task_id.encode("utf-8")
        payload = task_id_bytes + b"\x00" + fn_args_bytes
        
        msg = Message(
            msg_type=MessageType.TASK_SUBMIT,
            sender_id=self.node_id,
            payload=payload,
        )
        
        self.transport.sendto(msg.encode(), (target_host, target_port))

        try:
            result = await asyncio.wait_for(fut, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self.pending_tasks.pop(task_id, None)
            raise TimeoutError(f"Task {task_id} execution on {target_host}:{target_port} timed out")

    async def _handle_datagram(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            msg = Message.decode(data)
        except Exception as e:
            logger.warning(f"[%s] Received corrupted datagram from {addr}: {e}")
            return

        self.peers[msg.sender_id] = addr

        if msg.msg_type == MessageType.PING:
            await self._handle_ping(msg, addr)
        elif msg.msg_type == MessageType.PONG:
            self._handle_pong(msg)
        elif msg.msg_type == MessageType.TASK_SUBMIT:
            asyncio.create_task(self._handle_task_submit(msg, addr))
        elif msg.msg_type == MessageType.TASK_RESULT:
            self._handle_task_result(msg)

    async def _handle_ping(self, msg: Message, addr: Tuple[str, int]) -> None:
        pong_msg = Message(
            msg_type=MessageType.PONG,
            sender_id=self.node_id,
            payload=b"",
            msg_id=msg.msg_id,
        )
        if self.transport:
            self.transport.sendto(pong_msg.encode(), addr)

    def _handle_pong(self, msg: Message) -> None:
        pending = self.pending_pings.pop(msg.msg_id, None)
        if pending:
            fut, send_time = pending
            rtt = time.perf_counter() - send_time
            if not fut.done():
                fut.set_result(rtt)

    async def _handle_task_submit(self, msg: Message, addr: Tuple[str, int]) -> None:
        try:
            delimiter_idx = msg.payload.index(b"\x00")
            task_id = msg.payload[:delimiter_idx].decode("utf-8")
            fn_args_bytes = msg.payload[delimiter_idx + 1 :]
        except Exception as e:
            logger.error(f"[%s] Malformed TASK_SUBMIT payload: {e}")
            return

        task_result: TaskResult = await self.executor.execute_task(task_id, fn_args_bytes)
        
        success_byte = b"\x01" if task_result.success else b"\x00"
        if task_result.success:
            body = task_result.result_bytes
        else:
            body = (task_result.error or "Unknown error").encode("utf-8")

        res_payload = task_id.encode("utf-8") + b"\x00" + success_byte + body
        
        res_msg = Message(
            msg_type=MessageType.TASK_RESULT,
            sender_id=self.node_id,
            payload=res_payload,
            msg_id=msg.msg_id,
        )

        if self.transport:
            self.transport.sendto(res_msg.encode(), addr)

    def _handle_task_result(self, msg: Message) -> None:
        try:
            delimiter_idx = msg.payload.index(b"\x00")
            task_id = msg.payload[:delimiter_idx].decode("utf-8")
            success_byte = msg.payload[delimiter_idx + 1 : delimiter_idx + 2]
            body = msg.payload[delimiter_idx + 2 :]
        except Exception as e:
            logger.error(f"[%s] Malformed TASK_RESULT payload: {e}")
            return

        fut = self.pending_tasks.pop(task_id, None)
        if not fut or fut.done():
            return

        success = (success_byte == b"\x01")
        if success:
            try:
                result = TaskSerializer.deserialize_result(body)
                fut.set_result(result)
            except Exception as e:
                fut.set_exception(RuntimeError(f"Failed to deserialize result: {e}"))
        else:
            error_str = body.decode("utf-8", errors="replace")
            fut.set_exception(RuntimeError(f"Remote Task Failed: {error_str}"))
