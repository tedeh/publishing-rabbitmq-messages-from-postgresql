const assert = require('assert');
const { spawn } = require('child_process');
const delay = require('delay');
const services = require('./services');

const LISTENERS = parseInt(process.env.LISTENERS, 10) || 2;
const PUBLISH_COUNT = parseInt(process.env.PUBLISH_COUNT, 10) || 100;
const JITTER = parseInt(process.env.JITTER, 10) || 7;
const AFTER_PUBLISH_DELAY = parseInt(process.env.AFTER_PUBLISH_DELAY, 10) || PUBLISH_COUNT * JITTER;

test();

async function test () {
  const pg = await services.pg();
  const amqp = await services.amqp();

  const ch = await amqp.createChannel();

  // setup a queue
  await ch.assertQueue('queue_message_test_log');
  await ch.purgeQueue('queue_message_test_log');
  await ch.bindQueue('queue_message_test_log', 'amq.direct', 'queue_message_test');

  const received = {}; // every message value stored here
  await ch.consume('queue_message_test_log', msg => {
    const json = JSON.parse(msg.content);
    assert.ok(typeof json.value === 'number');
    received[json.value] = (received[json.value] || 0) + 1;
  });

  // spawn sub-processes "listeners"
  const children = [];
  for (let i = 0; i < LISTENERS; i++) {
    const child = spawn('node', ['./listen.js']);
    child.stdout.on('data', data => {
      process.stdout.write(`${i} out: ${data}`);
    });
    child.stderr.on('data', data => {
      process.stderr.write(`${i} err: ${data}`);
    });
    console.log(`spawned listener #${i}`);
    children.push(child);
  }

  await pg.query(`CREATE TEMP SEQUENCE queue_message_test_seq MINVALUE 0 START WITH 0`);
  await pg.query('TRUNCATE queue_message');

  for (let i = 0; i < PUBLISH_COUNT; i++) {
    await pg.query(`
      INSERT INTO queue_message (exchange, routing_key, content)
      VALUES (
        'amq.direct',
        'queue_message_test',
        jsonb_build_object('value', nextval('queue_message_test_seq'))
      );
    `);
    await delay(Math.random() * JITTER);
  }

  // we wait a fixed amount of time here to give every listener a chance to finish
  await delay(AFTER_PUBLISH_DELAY);

  for (const i in children) {
    const child = children[i];
    child.kill();
    console.log(`killed listener #${i}`);
  }

  // check what was received
  const duplicates = [];
  const unreceived = [];
  for (let i = 0; i < PUBLISH_COUNT; i++) {
    if (typeof received[i] === 'undefined') {
      unreceived.push(i);
    } else if (received[i] > 1) {
      duplicates.push(i);
    }
  }
  assert.ok(unreceived.length === 0, `error: did not receive ${unreceived.join(', ')}`);
  console.log('pass: all messages received');
  assert.ok(duplicates.length === 0, `error: duplicates received ${duplicates.join(', ')}`);
  console.log('pass: no duplicates received');
  process.exit();
}
