import logging
import os
import re

import tornado.ioloop

from pyworker.tornado import MessageQueueDispatcher


def store_title(correlation_id, msg):
    pattern = re.compile(r'^Title: (.*)$')
    filename = msg['filename']
    title = ''

    with open(filename, 'r') as fp:
        lineno = 0
        current = None
        while True:
            line = fp.readline()
            if not line:
                break
            m = pattern.match(line)
            if m:
                title = m.group(1)
                break
            lineno += 1

    return dict(filename=filename, title=title)


def main():
    logger = logging.getLogger('MessageQueueDispatcher')
    logger.setLevel(logging.DEBUG)

    dispatcher = MessageQueueDispatcher(
        os.environ['AMQP_SERVER'], "store-title", store_title
    )
    dispatcher.connect()

    ioloop = tornado.ioloop.IOLoop.current()
    ioloop.start()


if __name__ == '__main__':
    main()
