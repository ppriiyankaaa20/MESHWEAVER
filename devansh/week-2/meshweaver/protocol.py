from dataclasses import dataclass
from enum import IntEnum
import struct
import uuid
from typing import Optional

MAGIC_BYTES = b"MW01"
HEADER_FIXED_FORMAT = "!4sB16sHI"
HEADER_FIXED_SIZE = struct.calcsize(HEADER_FIXED_FORMAT)


class MessageType(IntEnum):
    PING = 0x01
    PONG = 0x02
    TASK_SUBMIT = 0x03
    TASK_RESULT = 0x04
    FIND_NODE = 0x05
    FIND_NODE_RESPONSE = 0x06
    GOSSIP_LOAD = 0x07


@dataclass
class Message:
    msg_type: MessageType
    sender_id: str
    payload: bytes
    msg_id: str = ""

    def __post_init__(self):
        if not self.msg_id:
            self.msg_id = str(uuid.uuid4())

    def encode(self) -> bytes:
        msg_id_bytes = uuid.UUID(self.msg_id).bytes
        sender_bytes = self.sender_id.encode("utf-8")
        sender_len = len(sender_bytes)
        payload_len = len(self.payload)

        header = struct.pack(
            HEADER_FIXED_FORMAT,
            MAGIC_BYTES,
            int(self.msg_type),
            msg_id_bytes,
            sender_len,
            payload_len,
        )

        return header + sender_bytes + self.payload

    @classmethod
    def decode(cls, data: bytes) -> "Message":
        if len(data) < HEADER_FIXED_SIZE:
            raise ValueError(f"Datagram too short: {len(data)} bytes (min {HEADER_FIXED_SIZE})")

        magic, msg_type_raw, msg_id_bytes, sender_len, payload_len = struct.unpack(
            HEADER_FIXED_FORMAT, data[:HEADER_FIXED_SIZE]
        )

        if magic != MAGIC_BYTES:
            raise ValueError(f"Invalid magic bytes: {magic!r}")

        total_expected = HEADER_FIXED_SIZE + sender_len + payload_len
        if len(data) < total_expected:
            raise ValueError(
                f"Incomplete payload: got {len(data)} bytes, expected {total_expected}"
            )

        offset = HEADER_FIXED_SIZE
        sender_id = data[offset : offset + sender_len].decode("utf-8")
        offset += sender_len

        payload = data[offset : offset + payload_len]
        msg_id = str(uuid.UUID(bytes=msg_id_bytes))

        return cls(
            msg_type=MessageType(msg_type_raw),
            sender_id=sender_id,
            payload=payload,
            msg_id=msg_id,
        )


@dataclass
class TaskPayload:
    task_id: str
    serialized_function: bytes

    @classmethod
    def create(cls, task_id: str, serialized_function: bytes) -> "TaskPayload":
        return cls(task_id=task_id, serialized_function=serialized_function)


@dataclass
class TaskResult:
    task_id: str
    success: bool
    result_bytes: bytes
    error: Optional[str] = None
