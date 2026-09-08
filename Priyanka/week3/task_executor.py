import cloudpickle


class TaskExecutor:

    def __init__(
        self,
        node_id,
        transport
    ):

        self.node_id = node_id
        self.transport = transport

    # ============================================================
    # EXECUTE TASK
    # ============================================================

    async def execute_task(
        self,
        data
    ):

        task_package = None

        try:

            task_package = cloudpickle.loads(
                data
            )

            task_id = task_package[
                "task_id"
            ]

            task = task_package[
                "task"
            ]

            args = task_package.get(
                "args",
                ()
            )

            reply_addr = task_package.get(
                "reply_addr"
            )

            router_addr = task_package.get(
                "router_addr"
            )

            print(
                "\n========== TASK EXECUTION =========="
            )

            print(
                f"Worker Node : {self.node_id}"
            )

            print(
                f"Task ID     : {task_id}"
            )

            print(
                f"Function    : {task.__name__}"
            )

            print(
                f"Arguments   : {args}"
            )

            print(
                "===================================="
            )

            # Execute
            result = task(
                *args
            )

            print(
                f"[EXECUTOR] "
                f"Task {task_id} completed"
            )

            print(
                f"[EXECUTOR] Result = {result}"
            )

            # ----------------------------------------------------
            # Result
            # ----------------------------------------------------

            response = {

                "type": "TASK_RESULT",

                "task_id": task_id,

                "success": True,

                "result": result,

                "worker": self.node_id
            }

            result_data = cloudpickle.dumps(
                response
            )

            # ----------------------------------------------------
            # Send result to sender
            # ----------------------------------------------------

            if reply_addr:

                self.transport.sendto(
                    result_data,
                    tuple(reply_addr)
                )

            return result_data

        except Exception as e:

            print(
                f"[EXECUTOR ERROR] {e}"
            )

            task_id = "unknown"

            if task_package:

                task_id = task_package.get(
                    "task_id",
                    "unknown"
                )

            response = {

                "type": "TASK_RESULT",

                "task_id": task_id,

                "success": False,

                "error": str(e),

                "worker": self.node_id
            }

            return cloudpickle.dumps(
                response
            )