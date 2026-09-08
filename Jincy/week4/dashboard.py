"""Live Rich dashboard for a running secure mesh."""

import asyncio

from rich.console import Group
from rich.live import Live
from rich.table import Table

from secure_mesh import topology_snapshot


def render_dashboard(nodes):
    snapshot = topology_snapshot(nodes)
    topology = Table(title="Mesh topology")
    topology.add_column("Node")
    topology.add_column("Listen")
    topology.add_column("Peers")
    topology.add_column("Tasks")
    for name, state in snapshot.items():
        tasks = ", ".join(f"{task}: {status}" for task, status in state["tasks"].items()) or "idle"
        topology.add_row(name, str(state["port"]), ", ".join(state["peers"]) or "none", tasks)
    return Group(topology)


async def dashboard_loop(nodes, refresh=0.5):
    with Live(render_dashboard(nodes), refresh_per_second=4) as live:
        while True:
            await asyncio.sleep(refresh)
            live.update(render_dashboard(nodes))