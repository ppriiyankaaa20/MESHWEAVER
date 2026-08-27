import asyncio
import inspect
import traceback
from typing import Optional, Tuple
from .serializer import TaskSerializer
from .protocol import TaskResult


class TaskExecutor:
    def __init__(self):
        pass

    async def execute_task(self, task_id: str, payload_bytes: bytes) -> TaskResult:
        try:
            func, args, kwargs = TaskSerializer.deserialize_task(payload_bytes)
        except Exception as e:
            return TaskResult(
                task_id=task_id,
                success=False,
                result_bytes=b"",
                error=f"Task Deserialization Failed: {type(e).__name__}: {str(e)}",
            )

        loop = asyncio.get_running_loop()

        try:
            if inspect.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = await loop.run_in_executor(None, lambda: func(*args, **kwargs))

            result_bytes = TaskSerializer.serialize_result(result)
            return TaskResult(
                task_id=task_id,
                success=True,
                result_bytes=result_bytes,
                error=None,
            )

        except Exception as e:
            return TaskResult(
                task_id=task_id,
                success=False,
                result_bytes=b"",
                error=f"{type(e).__name__}: {str(e)}",
            )
