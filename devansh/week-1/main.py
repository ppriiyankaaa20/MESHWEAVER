import asyncio
import logging
import math
import sys
from meshweaver import MeshNode


def remote_factorial(n: int) -> int:
    return math.factorial(n)


def remote_matrix_multiply(matrix_a: list, matrix_b: list) -> list:
    result = [[0 for _ in range(len(matrix_b[0]))] for _ in range(len(matrix_a))]
    for i in range(len(matrix_a)):
        for j in range(len(matrix_b[0])):
            for k in range(len(matrix_b)):
                result[i][j] += matrix_a[i][k] * matrix_b[k][j]
    return result


def remote_faulty_task():
    raise RuntimeError("Intentional edge compute node exception!")


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    print("===============================================================")
    print("        MeshWeaver P2P Task Broker - Week 1 Demonstration      ")
    print("===============================================================")

    node_alpha = MeshNode(host="127.0.0.1", port=9001, node_id="Node-Alpha")
    node_beta = MeshNode(host="127.0.0.1", port=9002, node_id="Node-Beta")

    await node_alpha.start()
    await node_beta.start()

    try:
        print("\n--- 1. P2P Mesh Ping / Pong Latency Verification ---")
        rtt = await node_alpha.ping(node_beta.host, node_beta.port)
        print(f"[OK] Node-Alpha successfully pinged Node-Beta!")
        print(f"     Round Trip Time (RTT): {rtt * 1000:.3f} ms")
        print(f"     Node-Beta Discovered Peers: {node_beta.peers}")

        print("\n--- 2. Remote Task Execution (Top-Level Function) ---")
        print("Dispatching 'remote_factorial(10)' from Node-Alpha to Node-Beta...")
        fact_res = await node_alpha.submit_task(
            node_beta.host, node_beta.port, remote_factorial, 10
        )
        print(f"[OK] Result received from Node-Beta: 10! = {fact_res}")

        print("\n--- 3. Remote Task Execution (Anonymous Lambda Function) ---")
        print("Dispatching anonymous lambda (x, y) -> x**2 + y**2...")
        lambda_task = lambda x, y: x**2 + y**2
        lambda_res = await node_alpha.submit_task(
            node_beta.host, node_beta.port, lambda_task, 12, 5
        )
        print(f"[OK] Result received from Node-Beta: 12^2 + 5^2 = {lambda_res}")

        print("\n--- 4. Remote Task Execution (Complex Data & Matrix Multiply) ---")
        mat_a = [[1, 2], [3, 4]]
        mat_b = [[5, 6], [7, 8]]
        print(f"Dispatching matrix multiply of {mat_a} x {mat_b}...")
        mat_res = await node_alpha.submit_task(
            node_beta.host, node_beta.port, remote_matrix_multiply, mat_a, mat_b
        )
        print(f"[OK] Result received from Node-Beta: Matrix Result = {mat_res}")

        print("\n--- 5. Exception Handling & Remote Stack Trace Propagation ---")
        print("Dispatching faulty task designed to raise an exception...")
        try:
            await node_alpha.submit_task(
                node_beta.host, node_beta.port, remote_faulty_task
            )
        except RuntimeError as e:
            print(f"[OK] Successfully intercepted remote exception:")
            print(f"     {e}")

        print("\n===============================================================")
        print("      Week 1 Verification Complete: All Features Passed!       ")
        print("===============================================================")

    finally:
        await node_alpha.stop()
        await node_beta.stop()


if __name__ == "__main__":
    asyncio.run(main())
