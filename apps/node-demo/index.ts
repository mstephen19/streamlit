import * as fs from 'fs/promises';
import { createServer } from 'https';
import pubSub, { redisEventBroker } from 'streamlit-node';

const redisBroker = redisEventBroker({
    options: {
        url: 'redis://localhost:6379',
    },
});

await redisBroker.initialize();

const pubSubApp = pubSub({
    eventBroker: redisBroker,
});

const testNamespace = pubSubApp.namespace('messages');

testNamespace.allowEventType('testmessage');

testNamespace.allowEventType('testmessage2');

const server = createServer(
    {
        cert: await fs.readFile('../../ssl/cert.pem'),
        key: await fs.readFile('../../ssl/cert.key'),
    },
    (req, res) => {
        // For demo purposes only
        res.setHeader('Access-Control-Allow-Origin', 'http://localhost:5173');
        res.setHeader('Access-Control-Allow-Credentials', 'true');

        if (req.method === 'OPTIONS') {
            res.writeHead(204);
            res.end();
            return;
        }

        pubSubApp.handler(req, res);
    }
);

server.listen(8000, () => {
    console.log('TLS server listening on port 8000');
});
