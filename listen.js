const pg = require('pg');
const createSubscriber = require('pg-listen');
const format = require('pg-format');
const amqplib = require('amqplib');

const client = new pg.Client();
const subscriber = createSubscriber();

const {
  AMQP_URL = 'amqp://guest:guest@127.0.0.1:5672'
} = process.env;
let amqp;

subscriber.events.on('error', err => {
  console.error(err.message);
});

subscriber.events.on('notification', ({channel, payload}) => {
  const { id } = payload;
  popQueueMessages(id).catch(err => {
    console.error(err.message);
  });
});

process.on('exit', () => {
  subscriber.close()
  client.end()
});

run().catch(err => {
  console.error(err.message);
});

async function run () {
  await client.connect();
  console.log('connected to postgres');
  await subscriber.connect();
  await subscriber.listenTo('queue_message_notify');
  console.log('listening on queue_message_notify');
  amqp = await amqplib.connect(AMQP_URL);
  amqp.on('error', err => {
    console.error('amqp error');
    console.error(err.message);
  });
  console.log('connected to amqp');
  await popQueueMessages();
}

async function popQueueMessages (id) {
  await client.query('BEGIN');

  let query =  'SELECT * FROM queue_message FOR UPDATE SKIP LOCKED';
  if (id) query = `SELECT * FROM queue_message WHERE id = $1 FOR UPDATE SKIP LOCKED`;
  const result = await client.query({
    text: query,
    values: id ? [id] : [],
  });
  console.log('got rows', result.rows.map(r => r.content.value));

  for (const row of result.rows) {
    await client.query('SAVEPOINT before_publish');
    try {
      const { exchange, routing_key, content, options } = row;

      const ch = await amqp.createConfirmChannel();

      // required by amqplib not to exit program on error
      ch.on('error', err => {
        console.error('channel error');
        console.error(err.message);
      });

      const str = JSON.stringify(content);
      console.log('publishing', row.id, exchange, routing_key, str, options);
      await ch.publish(exchange, routing_key, Buffer.from(str), options);
      await ch.waitForConfirms();
      await ch.close();

      await client.query({
        text: 'DELETE FROM queue_message WHERE id = $1',
        values: [row.id],
      });

      await client.query('RELEASE SAVEPOINT before_publish');
    } catch (err) {
      console.error('publishing error');
      console.error(err.message);
      await client.query('ROLLBACK TO before_publish');
    }
  }

  console.log('commit');
  await client.query('COMMIT');
}
