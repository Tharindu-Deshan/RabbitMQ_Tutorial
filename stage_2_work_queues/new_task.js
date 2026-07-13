#!/usr/bin/env node

const amqp = require('amqplib');

async function main() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  const queue = 'task_queue';
  const count = parseInt(process.argv[2], 10) || 10;

  await channel.assertQueue(queue, {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum'
    }
  });

  for (let i = 1; i <= count; i++) {
    const msg = `Hello World ${i}`;
    channel.sendToQueue(queue, Buffer.from(msg), {
      persistent: true
    });
    console.log(" [x] Sent '%s'", msg);
  }

  setTimeout(function() {
    connection.close();
    process.exit(0);
  }, 500);
}

main();