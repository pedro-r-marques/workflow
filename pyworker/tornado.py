""" workflow worker library using the tornado asynchronous library
"""
import pika
from pika.adapters.tornado_connection import TornadoConnection

from .dispatcher import BaseQueueDispatcher


class MessageQueueDispatcher(BaseQueueDispatcher):
    """ Register with the AMQP server and process incomimg messages from
        the workflow-manager.
    """

    def connect(self):
        TornadoConnection(
            pika.URLParameters(self.amqp_url),
            self.on_connection_open,
            self.on_connection_open_error,
            self.on_connection_closed
        )
