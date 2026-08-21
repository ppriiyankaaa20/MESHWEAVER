import hashlib
import socket


def generate_node_id():

    hostname = socket.gethostname()

    node_id = hashlib.sha1(
        hostname.encode()
    ).hexdigest()

    return node_id


node_id = generate_node_id()

print("================================")
print("     MeshWeaver DHT Node")
print("================================")

print("Hostname:", socket.gethostname())
print("Node ID :", node_id)