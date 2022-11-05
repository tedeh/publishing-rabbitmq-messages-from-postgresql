const amqplib = require('amqplib');
const pgListen = require('pg-listen');
const pg = require('pg');

const AMQP_URL = process.env.AMQP_URL || 'amqp://guest:guest@127.0.0.1:5672';

exports.pg = async function () {
  const pgClient = new pg.Pool();
  process.on('exit', () => pgClient.end());
  console.log('connected to postgres');
  return pgClient;
};

exports.amqp = async function () {
  const amqp = await amqplib.connect(AMQP_URL);
  amqp.on('error', err => {
    console.error('amqp error');
    console.error(err.message);
  });
  console.log('connected to amqp');
  return amqp;
};

exports.pgListen = async function () {
  const pgListenClient = pgListen();
  process.on('exit', () => pgListenClient.close());
  await pgListenClient.connect();
  console.log('pgListen connected to postgres');
  return pgListenClient;
};
