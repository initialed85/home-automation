from zmote.connector import Connector, HTTPTransport


class AirconClient(object):
    def __init__(self, ip):
        self._ip = ip

        self._transport = HTTPTransport(
            ip=ip,
        )

        self._connector = Connector(
            transport=self._transport,
        )

        self.connect = self._connector.connect
        self.learn = self._connector.learn
        self.send = self._connector.send
        self.disconnect = self._connector.disconnect
