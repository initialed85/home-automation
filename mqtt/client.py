import time
from collections import namedtuple
from queue import Empty, Queue
from threading import RLock, Thread

from paho.mqtt.client import Client


class NotConnectedError(Exception):
    pass


class UnknownTopicError(Exception):
    pass


Topic = namedtuple('Topic', ['topic', 'qos', 'queue', 'client'])

Message = namedtuple('Message', ['timestamp', 'topic', 'payload', 'retain', 'qos'])


class MQTTClient(object):
    def __init__(self, host, port, keepalive):
        self._host = host
        self._port = port
        self._keepalive = keepalive

        self._client = Client()

        self._loop_thread = None

        self._topics = {}
        self._topic_lock = RLock()

    def _on_connect(self, client, userdata, flags, rc):
        with self._topic_lock:
            for topic in self._topics.values():
                self.subscribe(
                    topic=topic.topic,
                    qos=topic.qos,
                    force_subscribe_if_existing=True,  # needed for reconnects
                )

    def _on_message(self, client, userdata, msg):
        with self._topic_lock:
            topic = self._topics.get(msg.topic)

        if topic is None:
            raise UnknownTopicError(
                'received a message for topic {0} but we don\'t have a local topic object for it'.format(
                    repr(msg.topic)
                )
            )

        if topic.queue is not None:
            message = Message(
                timestamp=msg.timestamp,
                topic=msg.topic,
                payload=msg.payload,
                retain=msg.retain,
                qos=msg.qos,
            )

            topic.queue.put(message)

    def connect(self):
        if self._loop_thread is not None:
            return

        self._client.connect(
            host=self._host,
            port=self._port,
            keepalive=self._keepalive,
        )

        self._client.on_message = self._on_message
        self._client.on_connect = self._on_connect

        self._loop_thread = Thread(
            target=self._client.loop_forever
        )

        self._connected = True
        self._loop_thread.start()

    def publish(self, topic, payload=None, retain=False, qos=0):
        if not self._connected:
            raise NotConnectedError('cannot publish, {0} is not connected'.format(self._client))

        return self._client.publish(
            topic=topic,
            payload=payload,
            retain=retain,
            qos=qos
        )

    def subscribe(self, topic, qos=0, disable_queue=False, force_subscribe_if_existing=False):
        with self._topic_lock:
            existing_topic_object = self._topics.get(topic)
            if existing_topic_object is None:
                topic_object = Topic(
                    topic=topic,
                    qos=qos,
                    queue=Queue() if not disable_queue else None,
                    client=self,
                )
            else:
                topic_object = existing_topic_object

            if self._loop_thread is not None:
                if existing_topic_object is None or force_subscribe_if_existing:
                    self._client.subscribe(
                        topic=topic_object.topic,
                        qos=topic_object.qos,
                    )

            self._topics = {
                topic: topic_object
            }

            return topic_object

    def get_topic(self, topic):
        with self._topic_lock:
            if topic not in self._topics:
                raise UnknownTopicError(
                    'cannot get local topic object for topic {0} because it doesn\'t exist'.format(
                        repr(topic)
                    )
                )

    def disconnect(self):
        if self._loop_thread is None:
            return

        self._client.disconnect()

        self._loop_thread.join()

        self._loop_thread = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()


def create_mqtt_client(host, port=1883, keepalive=60):
    return MQTTClient(
        host=host,
        port=port,
        keepalive=keepalive,
    )
