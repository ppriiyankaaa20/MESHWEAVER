import json
import time

from config import PING, PONG, TASK, RESULT, ERROR


def create_message(message_type, data=None):
    return {
        "type": message_type,
        "timestamp": time.time(),
        "data": data or {}
    }


def encode_message(message):
    return json.dumps(message).encode("utf-8")


def decode_message(data):
    if isinstance(data, bytes):
        data = data.decode("utf-8")

    return json.loads(data)


def create_ping(node_id):
    return create_message(
        PING,
        {
            "node_id": node_id
        }
    )


def create_pong(node_id):
    return create_message(
        PONG,
        {
            "node_id": node_id
        }
    )


def create_task(task_id, payload):
    return create_message(
        TASK,
        {
            "task_id": task_id,
            "payload": payload
        }
    )


def create_result(task_id, result):
    return create_message(
        RESULT,
        {
            "task_id": task_id,
            "result": result
        }
    )


def create_error(task_id, error):
    return create_message(
        ERROR,
        {
            "task_id": task_id,
            "error": str(error)
        }
    )