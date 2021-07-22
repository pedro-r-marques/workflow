import logging
import os
import pickle
import tempfile

import tornado.ioloop

from pyworker.tornado import MessageQueueDispatcher


def sum_splits(correlation_id, msg):
    job_id = correlation_id.split(':', maxsplit=1)[0]
    sections = msg['sections']
    sum_counts = {}
    for section in sections:
        with open(section, 'rb') as fp:
            word_counts = pickle.load(fp)
        for k, v in word_counts.items():
            sum_counts[k] = sum_counts.get(k, 0) + v

    output = os.path.join(tempfile.gettempdir(), job_id + '_counts.tsv')
    with open(output, 'w') as fp:
        for k, v in sum_counts.items():
            fp.write('\t'.join([k, str(v)]) + '\n')
    return dict(sum_splits=output)


def main():
    logger = logging.getLogger('MessageQueueDispatcher')
    logger.setLevel(logging.DEBUG)

    dispatcher = MessageQueueDispatcher(
        os.environ['AMQP_SERVER'], "sum-splits", sum_splits
    )
    dispatcher.connect()

    ioloop = tornado.ioloop.IOLoop.current()
    ioloop.start()


if __name__ == '__main__':
    main()
