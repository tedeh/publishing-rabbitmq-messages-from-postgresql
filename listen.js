const services = require('./services');

run().catch(err => {
  console.error(err);
});

async function run () {
  const pg = await services.pg();
  const amqp = await services.amqp();
  const pgSubscriber = await services.pgListen();

  await pgSubscriber.listenTo('queue_message_notify');
  pgSubscriber.events.on('notification', ({channel, payload}) => {
    const { id } = payload;
    popOneQueueMessage({pg, amqp}, id).catch(err => {
      console.error(err);
    });
  });
  console.log('listening on queue_message_notify');

  const result = await pg.query(`SELECT id, content FROM queue_message`);
  for (const row of result.rows) {
    await popOneQueueMessage({pg, amqp}, row.id);
  }
}

async function popOneQueueMessage (deps, id) {
  const { pg, amqp } = deps;
  const pool = await pg.connect();

  try {
    await pool.query('BEGIN');

    const result = await pool.query({
      text: 'SELECT * FROM queue_message WHERE id = $1 FOR UPDATE SKIP LOCKED',
      values: [id],
    });

    const row = result?.rows[0];
    if (!row) {
      return false;
    }
    const { exchange, routing_key, content, options } = row;

    const ch = await amqp.createConfirmChannel();

    // required by amqplib not to exit program on error
    ch.on('error', err => {
      console.error(err);
    });

    const str = JSON.stringify(content);
    await ch.publish(exchange, routing_key, Buffer.from(str), options);
    await ch.waitForConfirms();
    await ch.close();

    await pool.query({
      text: 'DELETE FROM queue_message WHERE id = $1',
      values: [id],
    });

    await pool.query('COMMIT');
    return true;
  } catch (err) {
    console.error(err);
    await pool.query('ROLLBACK');
    return false;
  } finally {
    await pool.release();
  }
}
