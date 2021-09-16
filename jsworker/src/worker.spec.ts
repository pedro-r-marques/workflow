import * as assert from 'assert';

import QueueListener from './worker';

class Deferred<T> {
  readonly promise: Promise<T>;
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  resolve = (value: T) => {};
  // eslint-disable-next-line @typescript-eslint/no-explicit-any,@typescript-eslint/no-unused-vars
  reject = (value: any) => {};
  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }
}

interface ITestMessage {
  a: number;
  b: number;
}

describe('WorkerTests', () => {
  describe('#handler', () => {
    it('user specified handler should be called', async function () {
      const AMQP_SERVER = process.env.AMQP_SERVER;
      if (!AMQP_SERVER) {
        this.skip();
      }

      const handler = function (correlationId: string, data: ITestMessage) {
        return data;
      };

      const listener = new QueueListener(AMQP_SERVER, 'unittest', handler);
      listener.connect();

      const rcvPromise = new Deferred();

      const hndl_manager = (correlationId: string) => {
        rcvPromise.resolve(correlationId);
        return null;
      };

      const sender = new QueueListener(
        AMQP_SERVER,
        'mock_manager',
        hndl_manager
      );
      const scp = sender.connect();
      const obj = {
        a: 1,
        b: 2,
      };
      scp
        .then(() => {
          sender.channel?.publish(
            '',
            'unittest',
            Buffer.from(JSON.stringify(obj)),
            {
              persistent: true,
              correlationId: '1:step',
              replyTo: 'mock_manager',
            }
          );
        })
        .catch(error => {
          console.log('send error:', error);
          assert.fail();
        });
      const v = await rcvPromise.promise;
      assert.equal(v, '1:step');
      listener.stop();
      sender.stop();
    });

    it('async handler should be supported', async function () {
      const AMQP_SERVER = process.env.AMQP_SERVER;
      if (!AMQP_SERVER) {
        this.skip();
      }
      const handler = function (correlationId: string, data: ITestMessage) {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            resolve({a: data.a + 1, b: data.b + 1});
          }, 10);
        });
      };

      const listener = new QueueListener(AMQP_SERVER, 'unittest', handler);
      listener.connect();

      const rcvPromise = new Deferred();

      const hndl_manager = (correlationId: string, data: ITestMessage) => {
        rcvPromise.resolve(data);
        return null;
      };

      const sender = new QueueListener(
        AMQP_SERVER,
        'mock_manager',
        hndl_manager
      );
      const scp = sender.connect();
      const obj: ITestMessage = {
        a: 1,
        b: 2,
      };
      scp
        .then(() => {
          sender.channel?.publish(
            '',
            'unittest',
            Buffer.from(JSON.stringify(obj)),
            {
              persistent: true,
              correlationId: '1:step',
              replyTo: 'mock_manager',
            }
          );
        })
        .catch(error => {
          console.log('send error:', error);
          assert.fail();
        });
      const v = await rcvPromise.promise;
      assert.deepEqual(v, {a: 2, b: 3});
      listener.stop();
      sender.stop();
    });
  });
});
