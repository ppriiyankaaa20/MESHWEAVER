import cloudpickle
from typing import Any, Callable, Dict, Tuple


class TaskSerializer:
    @staticmethod
    def serialize_task(func: Callable, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> bytes:
        task_data = {
            "func": func,
            "args": args,
            "kwargs": kwargs,
        }
        return cloudpickle.dumps(task_data)

    @staticmethod
    def deserialize_task(payload_bytes: bytes) -> Tuple[Callable, Tuple[Any, ...], Dict[str, Any]]:
        task_data = cloudpickle.loads(payload_bytes)
        return task_data["func"], task_data["args"], task_data["kwargs"]

    @staticmethod
    def serialize_result(result: Any) -> bytes:
        return cloudpickle.dumps(result)

    @staticmethod
    def deserialize_result(result_bytes: bytes) -> Any:
        return cloudpickle.loads(result_bytes)
