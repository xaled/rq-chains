from typing import Optional, TYPE_CHECKING
import json

if TYPE_CHECKING:
    from redis import Redis


def publish_to_pubsub(connection: Optional['Redis'], channel: str, message_data: str|bytes):
    # message_str = json.dumps(message)
    connection.publish(channel, message_data)


def publish_to_stream(connection: Optional['Redis'], stream: str, message_dict: dict, **kwargs):
    connection.xadd(stream, message_dict, **kwargs)
