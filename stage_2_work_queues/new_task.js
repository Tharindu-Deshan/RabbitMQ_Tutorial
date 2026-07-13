#!/usr/bin/env node

const amqp = require('amqplib');
const fs = require('fs');

const PENDING_FILE = __dirname + '/pending.json';
const COUNTER_FILE = __dirname + '/next_id.json';
const SEND_DELAY_MS = 300;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function loadPending() {
  if (fs.existsSync(PENDING_FILE)) {
    return JSON.parse(fs.readFileSync(PENDING_FILE, 'utf8'));
  }
  return [];
}

function savePending(list) {
  if (list.length === 0) {
    if (fs.existsSync(PENDING_FILE)) fs.unlinkSync(PENDING_FILE);
    return;
  }
  fs.writeFileSync(PENDING_FILE, JSON.stringify(list, null, 2));
}

function loadNextId() {
  if (fs.existsSync(COUNTER_FILE)) {
    return JSON.parse(fs.readFileSync(COUNTER_FILE, 'utf8')).nextId;
  }
  return 1;
}

function saveNextId(nextId) {
  fs.writeFileSync(COUNTER_FILE, JSON.stringify({ nextId }, null, 2));
}

async function main() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createConfirmChannel();

  const queue = 'task_queue';
  const count = parseInt(process.argv[2], 10) || 10;

  await channel.assertQueue(queue, {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum'
    }
  });

  const pending = loadPending();
  if (pending.length > 0) {
    console.log(" [i] Found %d pending message(s) from last broken run, resending first", pending.length);
  }

  // ids never restart at 1 - each send attempt gets its own unique, ever
  // increasing id so retried and fresh messages can never collide on identity
  let nextId = loadNextId();
  const newMsgs = [];
  for (let i = 0; i < count; i++) {
    newMsgs.push({ id: nextId, text: `Hello World ${nextId}` });
    nextId++;
  }
  saveNextId(nextId);

  const toSend = pending.concat(newMsgs);
  const unconfirmed = new Map(toSend.map(m => [m.id, m.text]));

  // write-ahead: whole batch marked pending BEFORE we send a single one.
  // a hard kill (SIGKILL, crash, power loss) can't run any exit handler,
  // so the file on disk must already be correct at every point in time,
  // not just written reactively when we detect a break.
  savePending(toSend);

  process.on('SIGINT', function() {
    console.log("\n [!] Interrupted. %d message(s) not yet confirmed by broker", unconfirmed.size);
    connection.close();
    process.exit(1);
  });

  for (const { id, text } of toSend) {
    channel.sendToQueue(queue, Buffer.from(text), {
      persistent: true
    }, function(err) {
      if (err) {
        console.log(" [!] Broker NOT confirm '%s': %s", text, err.message);
      } else {
        console.log(" [x] Confirmed '%s'", text);
        unconfirmed.delete(id);
        savePending(Array.from(unconfirmed, ([id, text]) => ({ id, text })));
      }
    });
    await sleep(SEND_DELAY_MS);
  }

  await channel.waitForConfirms();
  connection.close();
  process.exit(0);
}

main();
