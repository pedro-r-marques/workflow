import * as fs from 'fs';
import * as readline from 'readline';
import * as os from 'os';
import * as path from 'path';
import QueueListener from 'jsworker';

interface IMessage {
  sections: string[];
}

interface IResult {
  sum_splits: string;
}

function readLine(line: string, counts: Map<string, number>) {
  const [key, valueStr] = line.split('\t');
  const value = parseInt(valueStr);
  const current = counts.has(key) ? counts.get(key) : 0;
  counts.set(key, current! + value);
}

function sumSplits(correlationId: string, msg: IMessage): Promise<IResult> {
  const counts = new Map<string, number>();

  const promises: Promise<string>[] = [];
  msg.sections.forEach(section => {
    const reader = readline.createInterface(fs.createReadStream(section));
    reader.on('line', line => readLine(line, counts));
    promises.push(
      new Promise((resolve, reject) => {
        reader.on('close', (err: Error) => {
          if (err) {
            reject(err);
            return;
          }
          resolve(section);
        });
      })
    );
  });

  const jobId = correlationId.split(':')[0];
  const output = path.join(os.tmpdir(), jobId + '_counts.tsv');

  const p = Promise.all(promises)
    .then(() => {
      let data = '';
      counts.forEach((value, key) => {
        data += `${key}\t${value}\n`;
      });
      return data;
    })
    .then(data => {
      return new Promise<IResult>((resolve, reject) => {
        fs.writeFile(output, data, err => {
          if (err) {
            reject(err);
            return;
          }
          resolve({sum_splits: output});
        });
      });
    });

  return p;
}

function main() {
  const amqpServer = process.env.AMQP_SERVER;
  if (!amqpServer) {
    console.log('No AMQP_SERVER env variable not defined');
  }
  const listener = new QueueListener(amqpServer!, 'sum-splits', sumSplits);
  listener.connect();
}

main();
