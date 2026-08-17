import asyncio
import cloudpickle


class MeshNode(asyncio.DatagramProtocol):

    def __init__(self):
        self.transport = None

    # -------------------------------------------------
    # NODE START
    # -------------------------------------------------

    def connection_made(self, transport):

        self.transport = transport

        address = transport.get_extra_info("sockname")

        print("=" * 50)
        print("MeshWeaver Node Started")
        print(f"Listening on: {address}")
        print("=" * 50)

    # -------------------------------------------------
    # RECEIVE TASK
    # -------------------------------------------------

    def datagram_received(self, data, addr):

        print(f"\nTask received from {addr}")

        # Create default task ID
        task_id = "UNKNOWN"

        try:

            # -----------------------------------------
            # Deserialize received task
            # -----------------------------------------

            task_package = cloudpickle.loads(data)

            # Get task ID
            task_id = task_package["task_id"]

            # Get function
            task = task_package["task"]

            # Get function arguments
            args = task_package.get("args", ())

            print(f"Task ID : {task_id}")

            # -----------------------------------------
            # Execute task
            # -----------------------------------------

            result = task(*args)

            print("Task executed successfully")
            print(f"Result : {result}")

            # -----------------------------------------
            # Successful response
            # -----------------------------------------

            response = {
                "task_id": task_id,
                "success": True,
                "result": result
            }

        except Exception as e:

            # -----------------------------------------
            # Handle task failure
            # -----------------------------------------

            print("Task execution failed")
            print(f"Error : {e}")

            response = {
                "task_id": task_id,
                "success": False,
                "error": str(e)
            }

        # ---------------------------------------------
        # Send response back to sender
        # ---------------------------------------------

        try:

            response_data = cloudpickle.dumps(response)

            self.transport.sendto(
                response_data,
                addr
            )

            print("Response sent to sender")

        except Exception as e:

            print(f"Failed to send response: {e}")

    # -------------------------------------------------
    # NETWORK ERROR
    # -------------------------------------------------

    def error_received(self, exc):

        print(f"Network error: {exc}")

    # -------------------------------------------------
    # NODE CONNECTION CLOSED
    # -------------------------------------------------

    def connection_lost(self, exc):

        print("Node connection closed")


# =====================================================
# START NODE
# =====================================================

async def start_node():

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: MeshNode(),
        local_addr=("127.0.0.1", 9999)
    )

    try:

        # Keep the node running
        await asyncio.Future()

    finally:

        transport.close()


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":

    try:

        asyncio.run(start_node())

    except KeyboardInterrupt:

        print("\nMeshWeaver node stopped.")