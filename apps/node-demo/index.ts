import * as fs from 'fs/promises';
import pubSub, { redisEventBroker } from 'streamlit-node';
import express from 'express';
import spdy from 'spdy';

const redisBroker = redisEventBroker({
    options: {
        url: 'redis://localhost:6379',
    },
});

await redisBroker.initialize();

const pubSubApp = pubSub({
    eventBroker: redisBroker,
});

const chats = pubSubApp.namespace('chats');

chats.allowEventType('testmessage');
chats.allowEventType('testmessage2');

const app = express();

app.use((_, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', 'http://localhost:5173');
    res.setHeader('Access-Control-Allow-Credentials', 'true');

    next();
});

app.route('/events').get(pubSubApp.handler).post(pubSubApp.handler);

spdy.createServer(
    {
        cert: await fs.readFile('../../ssl/cert.pem'),
        key: await fs.readFile('../../ssl/cert.key'),
    },
    app
).listen(8000, () => {
    console.log('TLS server listening on port 8000');
});
