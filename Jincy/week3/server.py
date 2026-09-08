import asyncio
import cloudpickle


class UDPServerProtocol(asyncio.DatagramProtocol):

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):

        # -------------------------
        # PING message
        # -------------------------
        if data == b"PING":

            print(f"Received from {addr}: PING")

            response = b"PONG"

            self.transport.sendto(response, addr)

            print("Sent: PONG")

        # -------------------------
        # TASK message
        # -------------------------
        else:

            print(f"Received task from {addr}")

            try:

                # Deserialize function + arguments
                task = cloudpickle.loads(data)

                function = task["function"]
                arguments = task["arguments"]

                print("Task received!")
                print("Executing task...")

                # Execute function
                result = function(*arguments)

                print("Task result:", result)

                # Send result back
                result_data = cloudpickle.dumps(result)

                self.transport.sendto(result_data, addr)

                print("Result sent to client.")

            except Exception as e:

                print("Task execution failed:", e)


async def main():

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol(),
        local_addr=("127.0.0.1", 8000)
    )

    print("===================================")
    print("MeshWeaver Node B started...")
    print("Listening on 127.0.0.1:8000")
    print("===================================")

    try:
        await asyncio.Future()

    finally:
        transport.close()


asyncio.run(main())