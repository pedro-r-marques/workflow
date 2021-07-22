import logging
import os
import pickle
import tempfile

import tornado.ioloop

from pyworker.tornado import MessageQueueDispatcher


def word_counts(correlation_id: str, msg):
    job_id = correlation_id.split(':', maxsplit=1)[0]
    filename = msg['filename']
    section = msg['section']
    word_counts = {}
    with open(filename, 'r') as fp:
        lineno = 0
        while True:
            line = fp.readline()
            if not line:
                break
            if lineno == section['end']:
                break

            if lineno >= section['start']:
                tokens = line.split()
                for tok in tokens:
                    word_counts[tok] = word_counts.get(tok, 0) + 1

            lineno += 1

    output = os.path.join(tempfile.gettempdir(), job_id + '_counts.bin')
    with open(output, 'wb') as fp:
        pickle.dump(word_counts, fp)

    # task end gathers results from all jobs
    return dict(section=output)


def main():
    logger = logging.getLogger('MessageQueueDispatcher')
    logger.setLevel(logging.DEBUG)
    dispatcher = MessageQueueDispatcher(
        os.environ['AMQP_SERVER'], "segment-word-counts", word_counts
    )
    dispatcher.connect()

    ioloop = tornado.ioloop.IOLoop.current()
    ioloop.start()


if __name__ == '__main__':
    main()
