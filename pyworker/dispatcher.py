import abc
import functools
import logging
import json
import sys

import pika


class BaseQueueDispatcher(abc.ABC):
    def __init__(self, amqp_url, queue_name, handler):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.handler = handler
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._logger = logging.getLogger(self.__class__.__name__)

    @abc.abstractmethod
    def connect(self):
        raise NotImplementedError

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        if self._connection is None:
            return
        self._logger.info('Closing connection')
        self._connection.close()

    def on_connection_open_error(self, connection, err):
        """Invoked on connection open failure."""
        self._logger.error('Connection open failed: %s', err)
        connection.ioloop.call_later(5, self.reconnect)

    def on_connection_closed(self, connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.

        """
        self._channel = None
        self._connection = None
        if not self._closing:
            self._logger.warning(
                'Connection closed, reopening in 5 seconds: %s', reason)
            connection.ioloop.call_later(5, self.reconnect)

    def on_connection_open(self, connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :param pika.SelectConnection _unused_connection: The connection

        """
        self._logger.info('Connection opened')
        self._connection = connection
        self.open_channel(connection)

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        if not self._closing:
            # Create a new connection
            self.connect()

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters.

        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed

        """
        self._logger.warning('Channel %r was closed: %s', channel, reason)
        self._channel = None
        self._connection.ioloop.call_later(
            1, self.open_channel, self._connection)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        self._logger.info('Channel opened')
        channel.add_on_close_callback(self.on_channel_closed)
        self._channel = channel
        self.setup_queue()

    def setup_queue(self):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        self._logger.info('Declaring queue %s', self.queue_name)
        self._channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            callback=self.on_queue_declareok,
        )

    def on_queue_declareok(self, method_frame):
        self.start_consuming()

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        self._logger.info('Consumer was cancelled remotely, shutting down: %r',
                          method_frame)
        if self._channel:
            self._channel.close()

    def _handler_callback(self, channel, basic_deliver, properties, future):
        """ The pika transport is designed such that writes are queues in
            the asynchronous select loop. This code uses a separate blocking
            connection to respond to the workflow-manager so that the workflow
            can proceed immediatly. Alternativly one could start a different
            connection on a different thread, which brings in complexity. Or
            modify the pika transport such that writes are sent immediatly
            unless the socket send returns EAGAIN.
        """
        ex = future.exception()
        if ex is not None:
            self._logger.exception(ex)
            channel.basic_nack(basic_deliver.delivery_tag)
            return

        response = future.result()
        channel.basic_ack(basic_deliver.delivery_tag)
        # null reply_to used in unittests only
        if properties.reply_to is None:
            return
        send_connection = pika.BlockingConnection(
            pika.URLParameters(self.amqp_url))
        rprop = pika.spec.BasicProperties(
            app_id=sys.argv[0],
            content_type="application/json",
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
            correlation_id=properties.correlation_id)
        send_channel = send_connection.channel()
        send_channel.basic_publish(
            "", properties.reply_to,
            json.dumps(response) if response is not None else None,
            properties=rprop)

    def on_message(self, channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body

        """
        self._logger.debug('Received message # %s from %s: %s',
                           basic_deliver.delivery_tag, properties.app_id, body)

        ioloop = self._connection.ioloop
        msg = json.loads(body) if body is not None else None
        fut = ioloop.run_in_executor(
            None, self.handler, properties.correlation_id, msg)
        hndl = functools.partial(
            self._handler_callback, channel, basic_deliver, properties)
        fut.add_done_callback(hndl)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        self._logger.info(
            'RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()
        self.close_connection()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            self._logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(
                consumer_tag=self._consumer_tag, callback=self.on_cancelok)

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.
        """
        self._logger.info('Issuing consumer related RPC commands')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(
            on_message_callback=self.on_message,
            queue=self.queue_name,
        )

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        self._logger.info('Closing the channel')
        self._channel.close()

    def open_channel(self, connection):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        self._logger.info('Creating a new channel')
        connection.channel(on_open_callback=self.on_channel_open)

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        self._closing = True
        self.stop_consuming()
