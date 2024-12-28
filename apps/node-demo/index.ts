import * as fs from 'fs/promises';
import pubSub, { redisEventBroker } from 'streamlit-node';
import express, { type Request } from 'express';
import https from 'https';
import cookie from 'cookie';

type Message = {
    sender: string;
    content: string;
};

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

chats.configureAuth({
    authorizeSubscriber(_, req) {
        const { nickname } = (req as Request).query;

        return Boolean(nickname);
    },
    setCookiesForSubscriber(keyName, req) {
        const { nickname } = (req as Request).query;

        return [`_session=${keyName}__${nickname}; Max-Age=3600; HttpOnly; Secure; SameSite=None`];
    },
    authorizePublisher(keyName, req) {
        const rawCookie = (req as Request).headers.cookie;
        if (!rawCookie) return false;

        const sessionCookie = cookie.parse(rawCookie)['_session'];
        if (!sessionCookie) return false;

        const [keyNameInCookie, nickname] = sessionCookie.split('__');

        return keyNameInCookie === keyName && Boolean(nickname);
    },
});

chats.allowEventType('message', {
    validateData(data, req) {
        try {
            const message = JSON.parse(data) as Message;
            if (!message.content) return false;

            const rawCookie = (req as Request).headers.cookie!;
            const sessionCookie = cookie.parse(rawCookie)['_session']!;

            const [_, nickname] = sessionCookie.split('__');
            message.sender = nickname!;

            return JSON.stringify(message);
        } catch (err) {
            return false;
        }
    },
});

const app = express();

app.use((_, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', 'http://localhost:5173');
    res.setHeader('Access-Control-Allow-Credentials', 'true');

    next();
});

app.route('/events').get(pubSubApp.handler).post(pubSubApp.handler);

https
    .createServer(
        {
            cert: await fs.readFile('../../ssl/cert.pem'),
            key: await fs.readFile('../../ssl/cert.key'),
        },
        app
    )
    .listen(8000, () => {
        console.log('TLS server listening on port 8000');
    });
