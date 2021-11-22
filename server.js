const {PubSub} = require('@google-cloud/pubsub');
require('dotenv').config();

const args = process.argv.slice(2);

let stored_messages = [];
let subscription = undefined;
let messageCount = 0;
let client = false;

async function publishMessage(data) {
    const pubsub = new PubSub();
    const dataBuffer = Buffer.from(data);
    const topic = 'test-topic';

    try {
        const messageId = await pubsub.topic(topic).publish(dataBuffer);
        console.log(`Message ${messageId} published.`)
        stored_messages.push(messageId);
    } catch(error) {
        console.error(error);
        process.exitCode = 1;
    }
}

const messageHandler = (message) => {
    console.log(`[${message.id}] - ${message.data}`)
    messageCount += 1;
    
    message.ack();
}

const initSubscriptions = () => {
    const pubsub = new PubSub();
    subscription = pubsub.subscription('test-topic-sub');

    console.log('Listening for messages...')
    subscription.on('message', messageHandler);
}

async function main() {
    const prompt = require('prompt-sync')({sigint: true});

    let quit = false;
    while(!quit) {
        let input = prompt("Message ('exit' to exit): ");

        if(input == 'exit') {
            quit = true;
        } else {
            await publishMessage(input);
        }
    }

}

if(args.length == 0) {
    console.log('Incorrect usage. Usage: node server.js [client/server]');
} else {
    if(args[0] == 'client') {
        client = true;
        initSubscriptions();
    } else if(args[0] == 'server') {
        main();
    } else {
        console.log('Unknown option. Use client/server.')
    }
}

process.on('SIGINT', () => {
    if(client) {
        subscription.removeListener('message', messageHandler);
        console.log(`\n\n${messageCount} message(s) received.`);
    }
    process.exit();
})