import logging
import os
import re

import tornado.ioloop

from pyworker.tornado import MessageQueueDispatcher


def handler(correlation_id, msg):
    pattern = re.compile(r'^CHAPTER [XIV]+\.$')
    filename = msg['filename']
    sections = []
    with open(filename, 'r') as fp:
        lineno = 0
        current = None
        while True:
            line = fp.readline()
            if not line:
                break
            if pattern.match(line):
                if current is not None:
                    sections.append(
                        dict(section=current[0], start=current[1], end=lineno))
                current = (line.strip(), lineno)
            lineno += 1

    if current is not None:
        sections.append(
            dict(section=current[0], start=current[1], end=lineno))

    # task creates a job per section array element
    return dict(filename=filename, sections=sections)


def main():
    logger = logging.getLogger('MessageQueueDispatcher')
    logger.setLevel(logging.DEBUG)
    dispatcher = MessageQueueDispatcher(
        os.environ['AMQP_SERVER'], "book-split", handler
    )

    dispatcher.connect()
    ioloop = tornado.ioloop.IOLoop.current()
    ioloop.start()


if __name__ == '__main__':
    main()
