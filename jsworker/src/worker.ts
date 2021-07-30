import * as os from 'os';
import * as amqplib from 'amqplib';

const reconnectTimeout = 30 * 1000; // 30s

/**
 * Message handler.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type MessageHandler = (correlationId: string, data: any) => any | Promise<any>;

/**
 * Message queue listener.
 */
class QueueListener {
  readonly amqpServer: string;
  readonly queueName: string;
  readonly handler: MessageHandler;
  private consumerTag?: string;
  connection?: amqplib.Connection;
  channel?: amqplib.Channel;
  private shutdown: boolean;

  /**
   * constructor
   *
   * @param {string} amqpServer AMQP server URI (e.g. amqp://guest:guest@localhost:5672/).
   * @param {string} queueName Worker messge queue.
   * @param {MessageHandler} handler Receive message handler.
   */
  constructor(amqpServer: string, queueName: string, handler: MessageHandler) {
    this.amqpServer = amqpServer;
    this.queueName = queueName;
    this.handler = handler;
    this.shutdown = false;
  }

  /**
   * Event handler for connection errors.
   *
   * @param {Error} err Received error
   * @return {void}
   */
  onConnectionError(err: Error) {
    if (this.shutdown) {
      return;
    }
    console.log('connection error', err);
    this.connection = undefined;
    this.channel = undefined;
    setTimeout(this.connect, reconnectTimeout);
  }

  /**
   * Event handler for channel errors.
   *
   * @param {Error} err Received error
   * @return {void}
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  onChannelError(err: Error) {
    if (this.shutdown) {
      return;
    }
    this.channel = undefined;
    setImmediate(() => {
      if (this.connection) {
        this.channelOpen(this.connection);
      }
    });
  }

  /**
   * Open a channel on an existing connection.
   *
   * @param {amqplib.Connection} connection Open connection.
   * @return {void}
   */
  channelOpen(connection: amqplib.Connection) {
    const channelPromise = connection
      .createChannel()
      .then(channel => {
        this.channel = channel;
        channel.on('close', error => {
          if (!this.shutdown) {
            console.log('channel close', error);
          }
          this.onChannelError(error);
        });
        return channel;
      })
      .catch(error => {
        console.log('createChannel error', error);
        this.onChannelError(error);
        return undefined;
      });

    const queuePromise = channelPromise.then(channel => {
      if (channel === undefined) {
        return Promise.reject(new Error('channel undefined'));
      }
      return channel.assertQueue(this.queueName, {durable: true});
    });
    return Promise.all([channelPromise, queuePromise])
      .then(values => {
        const channel = values[0];
        if (channel === undefined) {
          return Promise.reject(new Error('channel undefined'));
        }
        return channel.consume(this.queueName, this.onMessage.bind(this), {
          noAck: false,
        });
      })
      .then((result: amqplib.Replies.Consume) => {
        this.consumerTag = result.consumerTag;
      });
  }

  connect() {
    const channelPromise = amqplib
      .connect(this.amqpServer)
      .then(connection => {
        this.connection = connection;
        connection.on('close', this.onConnectionError.bind(this));
        return connection;
      })
      .then(connection => {
        return this.channelOpen(connection);
      })
      .catch(error => {
        if (this.connection === null) {
          console.log('connection error', error);
        } else {
          this.connection = undefined;
          console.log('createChannel error', error);
        }
        setTimeout(this.connect, reconnectTimeout);
      });
    return channelPromise;
  }

  stop() {
    this.shutdown = true;
    let promise: Promise<void>;
    if (this.channel) {
      promise = this.channel.cancel(this.consumerTag!).then(() => {
        if (this.channel === undefined) {
          return Promise.resolve();
        }
        const p = this.channel.close();
        this.channel = undefined;
        return p;
      });
    } else {
      promise = Promise.resolve();
    }
    return promise
      .then(() => {
        if (this.connection === undefined) {
          return Promise.resolve();
        }
        const p = this.connection.close();
        this.connection = undefined;
        return p;
      })
      .catch(error => {
        console.log('stop', error);
      });
  }

  private asyncHelper(message: amqplib.ConsumeMessage) {
    const data = JSON.parse(message.content.toString());
    try {
      const result = this.handler(message.properties.correlationId, data);
      if (result instanceof Promise) {
        return result;
      }
      return Promise.resolve(result);
    } catch (error) {
      return Promise.reject(error);
    }
  }

  onMessage(message: amqplib.ConsumeMessage | null) {
    if (!message) {
      return;
    }

    this.asyncHelper(message)
      .then(result => {
        this.channel?.ack(message);
        if (!result) {
          return;
        }
        this.channel?.publish(
          '',
          message.properties.replyTo,
          Buffer.from(JSON.stringify(result)),
          {
            appId: os.hostname(),
            persistent: true,
            correlationId: message.properties.correlationId,
          }
        );
      })
      .catch(error => {
        console.log(error);
        this.channel?.nack(message);
      });
  }
}

export default QueueListener;
