#!/usr/bin/env node

const amqp = require('amqplib');

async function main() {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    const queue = 'hello';
    const queue2='test_queue'
    const msg = 'Hello World!';
        const msg1 = 'Hello World! test 123';

    await channel.assertQueue(queue, {
        durable: true,
        arguments: { 'x-queue-type': 'quorum' }
    });
    channel.sendToQueue(queue, Buffer.from(msg));

    await channel.assertQueue(queue2, {
        durable: true,
        arguments: { 'x-queue-type': 'quorum' }
    });
    channel.sendToQueue(queue2, Buffer.from(msg1));

    console.log(" [x] Sent %s", msg);
    console.log(" [x] Sent %s", msg1);

    setTimeout(function() {
        connection.close();
        process.exit(0);
    }, 500);
}

main();