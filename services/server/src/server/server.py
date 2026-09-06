import socket
import logger
import safe_socket
import protocol
from lottery import Lottery

_LOTTERY_STORAGE_PATH = "bets.csv"

def receive_message(client_socket):
    header = safe_socket.recv_all(client_socket, protocol.TYPE_BYTES + protocol.SIZE_BYTES)

    msg_type, size = protocol.parse_header(header)

    value = safe_socket.recv_all(client_socket, size)

    return protocol.parse_message(msg_type, value)


def send_ack(client_socket):
    safe_socket.send_all(client_socket, protocol.encode_field(protocol.ACK_BYTES, b""))


def send_bets(client_socket, bets):
    safe_socket.send_all(client_socket, protocol.encode_bets(bets))


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(storage_path=_LOTTERY_STORAGE_PATH)

    def _handle_client(self, client_socket):
        action = "handle-client"
        bets = []
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                msg = receive_message(client_socket)

                if isinstance(msg, protocol.EndOfBets):
                    break

                bets.append(msg)
                send_ack(client_socket)

            self.lottery.store_bets(bets)

            winners = [
                bet for bet in self.lottery.load_bets()
                if bet.agency_id == msg.agency_id and self.lottery.has_won(bet)
            ]

            send_bets(client_socket, winners)

            logger.info(
                action, logger.LogResult.success, "bets", len(bets), "winners", len(winners)
            )
        except Exception as e:
            logger.error(action, logger.LogResult.fail, "bets", len(bets))
            raise e

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)
