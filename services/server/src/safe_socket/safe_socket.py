import socket


def recv_all(socket: socket.socket, size):
    data = socket.recv(size)
    if not data:
        raise ConnectionError("connection closed before receiving expected data")
    if len(data) != size:
        data += recv_all(socket, size - len(data))
    return data


def send_all(socket: socket.socket, bytes):
    sent = socket.send(bytes)
    if sent != len(bytes):
        send_all(socket, bytes[sent:])
