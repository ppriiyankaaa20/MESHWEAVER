import asyncio
import cloudpickle
import uuid


# ================================================================
# SAMPLE TASKS
# ================================================================

def add(a, b):

    return a + b


def multiply(a, b):

    return a * b


def divide(a, b):

    return a / b


# ================================================================
# TASK SENDER
# ================================================================

class TaskSender(asyncio.DatagramProtocol):

    def __init__(self):

        self.transport = None

        self.done = (
            asyncio.get_running_loop()
            .create_future()
        )

    # ============================================================
    # CONNECTION
    # ============================================================

    def connection_made(
        self,
        transport
    ):

        self.transport = transport

        print("\n" + "=" * 55)
        print("        MESHWEAVER WEEK 3 TASK SENDER")
        print("=" * 55)

        task_id = str(
            uuid.uuid4()
        )[:8]

        sender_address = (
            transport.get_extra_info(
                "sockname"
            )
        )

        # --------------------------------------------------------
        # Create task
        # --------------------------------------------------------

        task_package = {

            "task_id": task_id,

            "task": divide,

            "args": (
                10,
                2
            ),

            "reply_addr": sender_address,

            "execute_here": False
        }

        data = cloudpickle.dumps(
            task_package
        )

        # --------------------------------------------------------
        # Send to Node A
        # --------------------------------------------------------

        router_address = (
            "127.0.0.1",
            9001
        )

        self.transport.sendto(
            data,
            router_address
        )

        print(
            f"Task ID : {task_id}"
        )

        print(
            "Task sent to Node A"
        )

        print(
            "Waiting for result..."
        )

    # ============================================================
    # RECEIVE RESULT
    # ============================================================

    def datagram_received(
        self,
        data,
        addr
    ):

        try:

            response = cloudpickle.loads(
                data
            )

            if response.get(
                "type"
            ) != "TASK_RESULT":

                return

            print(
                "\n" + "=" * 55
            )

            print(
                "              TASK RESULT"
            )

            print(
                "=" * 55
            )

            print(
                f"Task ID     : "
                f"{response.get('task_id')}"
            )

            if response.get(
                "success"
            ):

                print(
                    "Status      : SUCCESS"
                )

                print(
                    f"Executed by : "
                    f"Node {response.get('worker')}"
                )

                print(
                    f"Result      : "
                    f"{response.get('result')}"
                )

            else:

                print(
                    "Status      : FAILED"
                )

                print(
                    f"Error       : "
                    f"{response.get('error')}"
                )

            print(
                "=" * 55
            )

            if not self.done.done():

                self.done.set_result(
                    response
                )

        except Exception as e:

            print(
                f"[SENDER ERROR] {e}"
            )

            if not self.done.done():

                self.done.set_exception(
                    e
                )


# ================================================================
# SEND TASK
# ================================================================

async def send_task():

    loop = asyncio.get_running_loop()

    transport, protocol = (
        await loop.create_datagram_endpoint(

            lambda: TaskSender(),

            local_addr=(
                "127.0.0.1",
                0
            )
        )
    )

    try:

        await protocol.done

    finally:

        transport.close()


# ================================================================
# MAIN
# ================================================================

if __name__ == "__main__":

    asyncio.run(
        send_task()
    )