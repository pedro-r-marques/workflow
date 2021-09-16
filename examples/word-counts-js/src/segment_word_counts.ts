import * as fs from 'fs';
import * as readline from 'readline';
import * as os from 'os';
import * as path from 'path';
import QueueListener from 'jsworker';

interface Section {
  start: number;
  end: number;
}
interface IMessage {
  filename: string;
  section: Section;
}

interface IResult {
  section: string;
}

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

async function wordCounts(
  correlationId: string,
  msg: IMessage
): Promise<IResult> {
  const reader = readline.createInterface(fs.createReadStream(msg.filename));
  const counts = new Map<string, number>();
  let lineno = 0;

  reader.on('line', line => {
    const accept = lineno >= msg.section.start && lineno < msg.section.end;
    if (accept) {
      const words = line.split(/\s+/);
      words.forEach(w => {
        if (!w) {
          return;
        }
        const value = counts.has(w) ? counts.get(w) : 0;
        counts.set(w, value! + 1);
      });
    }
    lineno = lineno + 1;
  });

  const jobId = correlationId.split(':')[0];
  const output = path.join(os.tmpdir(), jobId + '_s_counts.tsv');

  const deferred = new Deferred<IResult>();

  reader.on('close', () => {
    let data = '';
    counts.forEach((value, key) => {
      data += `${key}\t${value}\n`;
    });
    fs.writeFile(output, data, err => {
      if (err) {
        throw err;
      }
      deferred.resolve({section: output});
    });
  });

  return deferred.promise;
}

function main() {
  const amqpServer = process.env.AMQP_SERVER;
  if (!amqpServer) {
    console.log('No AMQP_SERVER env variable not defined');
  }
  const listener = new QueueListener(
    amqpServer!,
    'segment-word-counts',
    wordCounts
  );
  listener.connect();
}

main();
