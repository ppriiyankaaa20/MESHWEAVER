import asyncio
import cloudpickle
import uuid


# -------------------------------
# Tasks
# -------------------------------

def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


def divide(a, b):
    return a / b


# -------------------------------
# Task Sender
# -------------------------------

class TaskSender(asyncio.DatagramProtocol):

    def __init__(self):
        self.transport = None
        self.done = asyncio.get_running_loop().create_future()

    def connection_made(self, transport):

        self.transport = transport

        print("=" * 50)
        print("MeshWeaver Task Sender Started")
        print("=" * 50)

        task_id = str(uuid.uuid4())[:8]

        # Change this to test different tasks
        task_package = {
            "task_id": task_id,
            "task": divide,
            "args": (10, 2)
        }

        data = cloudpickle.dumps(task_package)

        self.transport.sendto(
            data,
            ("127.0.0.1", 9999)
        )

        print(f"Task ID : {task_id}")
        print("Task sent to Node B")

    def datagram_received(self, data, addr):

        try:

            response = cloudpickle.loads(data)

            task_id = response["task_id"]
            success = response["success"]

            print(f"\nTask ID : {task_id}")

            if success:

                result = response["result"]

                print("Task completed successfully")
                print(f"Result : {result}")

            else:

                error = response["error"]

                print("Task failed")
                print(f"Error : {error}")

            if not self.done.done():
                self.done.set_result(response)

        except Exception as e:

            print(f"Error processing response: {e}")

            if not self.done.done():
                self.done.set_exception(e)

    def error_received(self, exc):

        print(f"Network Error: {exc}")

        if not self.done.done():
            self.done.set_exception(exc)


# -------------------------------
# Start Sender
# -------------------------------

async def send_task():

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: TaskSender(),
        local_addr=("127.0.0.1", 0)
    )

    try:

        await protocol.done

    finally:

        transport.close()


if __name__ == "__main__":
    asyncio.run(send_task())