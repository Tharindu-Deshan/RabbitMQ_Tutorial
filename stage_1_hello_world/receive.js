#!/usr/bin/env node

const amqp = require('amqplib');

async function main() {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    const queue = 'hello';
    const queue2='test_queue'

    await channel.assertQueue(queue, {
        durable: true,
        arguments: { 'x-queue-type': 'quorum' }
    });

      await channel.assertQueue(queue2, {
        durable: true,
        arguments: { 'x-queue-type': 'quorum' }
    });

    console.log(" [*] Waiting for messages in %s and %s. To exit press CTRL+C", queue, queue2);

    channel.consume(queue, function(msg) {
        console.log(" [x] Received from %s: %s", queue, msg.content.toString());
    }, {
        noAck: true
    });

    channel.consume(queue2, function(msg) {
        console.log(" [x] Received from %s: %s", queue2, msg.content.toString());
    }, {
        noAck: true
    });
}

main();