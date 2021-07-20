import json
import os
import unittest
import logging

import tornado.ioloop
import pika

from .tornado import MessageQueueDispatcher


class MockManager(MessageQueueDispatcher):
    def __init__(self, ioloop, correlation_id):
        super().__init__(os.environ['AMQP_SERVER'], "mock_manager", None)
        self.ioloop = ioloop
        self.snd_correlation_id = correlation_id
        self.rcv_correlation_id = ''
        self.rcv_message = None

    def start_consuming(self):
        super().start_consuming()
        self.ioloop.add_callback(self.send_mock_message)

    def send_mock_message(self):
        msg = {
            'node': 'mock_manager'
        }

        properties = pika.spec.BasicProperties(
            correlation_id=self.snd_correlation_id,
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
            reply_to='mock_manager',
        )
        self._channel.basic_publish(
            "", "unittest", json.dumps(msg), properties)

    def on_message(self, channel, basic_deliver, properties, body):
        self.rcv_correlation_id = properties.correlation_id
        self.rcv_message = body
        self._channel.basic_ack(basic_deliver.delivery_tag)
        self.ioloop.add_callback(self.stop)

    def on_cancelok(self, unused_frame):
        super().on_cancelok(unused_frame)
        self.ioloop.stop()

    def run(self):
        self.connect()


@unittest.skipUnless(os.environ.get('AMQP_SERVER', ''),
                     "env variable AMQP_SERVER must be defined")
class QueueDispatcherTest(unittest.TestCase):
    def test_handler(self):
        def handler(correlation_id, data):
            data['response'] = True
            return data

        dispatcher = MessageQueueDispatcher(
            os.environ['AMQP_SERVER'], "unittest", handler
        )

        dispatcher.connect()
        ioloop = tornado.ioloop.IOLoop.current()
        mock_manager = MockManager(ioloop, "42")
        ioloop.add_callback(mock_manager.run)
        ioloop.start()

        self.assertEqual(mock_manager.rcv_correlation_id,
                         mock_manager.snd_correlation_id)
