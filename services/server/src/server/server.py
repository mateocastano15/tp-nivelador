import socket
import threading
import signal
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


def send_batch(client_socket, bets):
    safe_socket.send_all(client_socket, protocol.encode_batch(bets))


class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agency_quorum_min = agency_quorum_min
        self.lottery = Lottery(storage_path=_LOTTERY_STORAGE_PATH)
        self.finished_agencies = 0
        self.lock = threading.Lock()
        self.quorum_reached = threading.Event()
        self.shutdown_event = threading.Event()
        self.client_handlers = []

    def _handle_client(self, client_socket):
        action = "handle-client"
        bets = []
        with client_socket:
            try:
                logger.info(action, logger.LogResult.in_progress)
                while True:
                    msg = receive_message(client_socket)

                    if isinstance(msg, protocol.EndOfBets):
                        break

                    if isinstance(msg, list):
                        bets.extend(msg)
                    else:
                        bets.append(msg)
                    send_ack(client_socket)

                with self.lock:
                    logger.info("lock", logger.LogResult.success, "agency-id", msg.agency_id)
                    self.lottery.store_bets(bets)
                    self.finished_agencies += 1
                    if self.finished_agencies >= self.agency_quorum_min:
                        self.quorum_reached.set()
                logger.info("unlock", logger.LogResult.success, "agency-id", msg.agency_id)

                self.quorum_reached.wait()

                if self.shutdown_event.is_set():
                    logger.info(action, logger.LogResult.fail, "agency-id", msg.agency_id, "reason", "shutdown")
                    return

                with self.lock:
                    logger.info("lock", logger.LogResult.success, "agency-id", msg.agency_id)
                    winners = []
                    for bet in self.lottery.load_bets():
                        if bet.agency_id == msg.agency_id and self.lottery.has_won(bet):
                            winners.append(bet)
                logger.info("unlock", logger.LogResult.success, "agency-id", msg.agency_id)

                send_batch(client_socket, winners)

                logger.info(
                    action, logger.LogResult.success,
                    "agency-id", msg.agency_id, "bets", len(bets), "winners", len(winners)
                )
            except Exception as e:
                logger.error(action, logger.LogResult.fail, "bets", len(bets))
                raise e

    def _accept_connections(self, server_socket):
        action = "accept-connection"
        while True:
            try:
                logger.info(action, logger.LogResult.in_progress)
                client_socket, _ = server_socket.accept()
            except OSError:
                return
            logger.info(action, logger.LogResult.success)

            handler_thread = threading.Thread(target=self._handle_client, args=(client_socket,), daemon=True)
            with self.lock:
                self.client_handlers.append((handler_thread, client_socket))
            handler_thread.start()

    def run(self):
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()

            threading.Thread(
                target=self._accept_connections, args=(server_socket,), daemon=True
            ).start()

            signal.sigwait({signal.SIGTERM})
            logger.info("sigterm", logger.LogResult.in_progress)
            self.shutdown_event.set()
            self.quorum_reached.set()

        with self.lock:
            client_handlers = list(self.client_handlers)

        for _, client_socket in client_handlers:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        for handler_thread, _ in client_handlers:
            handler_thread.join()

        logger.info("shutdown", logger.LogResult.success)
