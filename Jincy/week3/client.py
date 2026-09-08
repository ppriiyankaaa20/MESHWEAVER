import asyncio
import cloudpickle


def add(a, b):
    return a + b


class UDPClientProtocol(asyncio.DatagramProtocol):

    def connection_made(self, transport):

        self.transport = transport

        print("MeshWeaver Node A started.")

        # Step 1: Send PING
        print("\nSending: PING")

        self.transport.sendto(
            b"PING",
            ("127.0.0.1", 8000)
        )

    def datagram_received(self, data, addr):

        # -------------------------
        # PONG received
        # -------------------------
        if data == b"PONG":

            print("Received: PONG")

            # Now send task
            self.send_task()

        # -------------------------
        # Task result received
        # -------------------------
        else:

            try:

                result = cloudpickle.loads(data)

                print("\nReceived task result:", result)

                print("\nTask completed successfully!")

                self.transport.close()

            except Exception as e:

                print("Could not read result:", e)

    def send_task(self):

        print("\nPreparing task...")

        # Function + arguments
        task = {
            "function": add,
            "arguments": (10, 20)
        }

        # Serialize task
        task_data = cloudpickle.dumps(task)

        print("Sending task: add(10, 20)")
        print("Serialized task size:", len(task_data), "bytes")

        # Send to server
        self.transport.sendto(
            task_data,
            ("127.0.0.1", 8000)
        )


async def main():

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(),
        local_addr=("127.0.0.1", 0)
    )

    try:

        await asyncio.Future()

    finally:

        transport.close()


asyncio.run(main())