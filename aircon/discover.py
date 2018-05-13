import copy
import socket
import struct
import time
from collections import namedtuple
from threading import Event, RLock, Thread

_GROUP = '239.255.250.250'
_RECEIVE_PORT = 9131
_SEND_PORT = 9130

Zmote = namedtuple('Zmote', ['uuid', 'type', 'make', 'model', 'revision', 'config_url', 'ip'])


class AirconDiscoverer(object):
    def __init__(self):
        self._receive_thread = None
        self._send_thread = None

        self._stop_event = None

        self._zmotes_lock = RLock()
        self._zmotes = {}

    @property
    def zmotes(self):
        with self._zmotes_lock:
            return copy.deepcopy(self._zmotes)

    def _send_loop(self):
        tx_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        while not self._stop_event.is_set():
            tx_sock.sendto(b'SENDAMXB', (_GROUP, _SEND_PORT))

            time.sleep(5)

    def _receive_loop(self):
        rx_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        rx_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        rx_sock.settimeout(1)

        mreq = struct.pack(
            "4sL",
            socket.inet_aton(_GROUP),
            socket.INADDR_ANY
        )

        rx_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        rx_sock.bind((_GROUP, _RECEIVE_PORT))

        while not self._stop_event.is_set():
            try:
                data, addr = rx_sock.recvfrom(1024)
            except socket.timeout:
                continue

            data = data.decode('utf-8')

            if 'AMXB<-' not in data:
                continue

            parts = data.rstrip('>').split('AMXB<-')[-1].split('><-')

            zmote_dict = {
                x[0].lower().replace('-', '_'): x[1] for x in [y.split('=') for y in parts]
            }

            zmote_dict['ip'] = zmote_dict['config_url'].split('http://')[-1]

            zmote = Zmote(**zmote_dict)

            with self._zmotes_lock:
                self._zmotes[zmote.uuid] = zmote

    def start(self):
        self._stop_event = Event()

        self._send_thread = Thread(
            target=self._send_loop
        )
        self._send_thread.start()

        time.sleep(0.1)

        self._receive_thread = Thread(
            target=self._receive_loop
        )
        self._receive_thread.start()

    def stop(self):
        self._stop_event.set()

        self._receive_thread.join()

        self._send_thread.join()


def create_aircon_discoverer():
    return AirconDiscoverer()
