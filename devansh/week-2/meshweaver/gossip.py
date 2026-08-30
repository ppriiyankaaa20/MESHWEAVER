import asyncio
from dataclasses import dataclass
import json
import logging
import time
from typing import Any, Dict, Optional

try:
    import psutil
except ImportError:
    psutil = None

logger = logging.getLogger("MeshWeaver.Gossip")


@dataclass
class NodeLoadState:
    node_id: str
    cpu_percent: float
    ram_percent: float
    timestamp: float

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "cpu_percent": self.cpu_percent,
            "ram_percent": self.ram_percent,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "NodeLoadState":
        return cls(
            node_id=data["node_id"],
            cpu_percent=float(data["cpu_percent"]),
            ram_percent=float(data["ram_percent"]),
            timestamp=float(data["timestamp"]),
        )


class GossipEngine:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.peer_loads: Dict[str, NodeLoadState] = {}
        self._gossip_task: Optional[asyncio.Task] = None
        self.gossip_interval = 5.0

    def get_local_load(self) -> NodeLoadState:
        if psutil:
            cpu = psutil.cpu_percent(interval=None)
            ram = psutil.virtual_memory().percent
        else:
            cpu = 5.0
            ram = 20.0

        return NodeLoadState(
            node_id=self.node_id,
            cpu_percent=cpu,
            ram_percent=ram,
            timestamp=time.time(),
        )

    def update_peer_load(self, load_state: NodeLoadState) -> None:
        self.peer_loads[load_state.node_id] = load_state

    def start(self, node_instance: Any, interval: float = 5.0) -> None:
        self.gossip_interval = interval
        if not self._gossip_task or self._gossip_task.done():
            self._gossip_task = asyncio.create_task(self._gossip_loop(node_instance))

    def stop(self) -> None:
        if self._gossip_task and not self._gossip_task.done():
            self._gossip_task.cancel()
            self._gossip_task = None

    async def _gossip_loop(self, node_instance: Any) -> None:
        while True:
            try:
                await asyncio.sleep(self.gossip_interval)
                load_state = self.get_local_load()
                self.update_peer_load(load_state)
                
                payload = json.dumps(load_state.to_dict()).encode("utf-8")
                
                contacts = node_instance.routing_table.get_all_contacts()
                for contact in contacts:
                    node_instance._send_gossip_to_addr(payload, (contact.host, contact.port))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[%s] Gossip loop error: %s", self.node_id, e)
