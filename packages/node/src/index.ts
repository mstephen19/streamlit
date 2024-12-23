import { TypedEmitter } from 'tiny-typed-emitter';
import { nanoid } from 'nanoid';
import { createClient, RedisClientType, type RedisClientOptions } from 'redis';
import assert from 'assert';

import type { IncomingMessage, ServerResponse } from 'http';

type Awaitable<T> = T | Promise<T>;

type EventInfo = {
    id: string;
    event: string;
    data: string;
};

// Sends a server-sent-event to the client
export type EventBrokerClient = (event: EventInfo) => void;

export type EventBroker = {
    subscribe: (namespaceName: string, keyName: string, client: EventBrokerClient) => Awaitable<() => Awaitable<void>>;
    publish: (namespaceName: string, keyName: string, event: EventInfo) => Awaitable<void>;
};

export const inMemoryEventBroker = (): EventBroker => {
    const namespaceMap: {
        [namespaceName: string]: {
            /**
             * Emitter for listening to events on a given key
             */
            eventEmitter: TypedEmitter<{
                [keyName: string]: (event: EventInfo) => void;
            }>;
            /**
             * When a new key is detected, it's added to this map
             */
            keyClientMap: {
                [keyName: string]: {
                    /**
                     * Removes the listener from the namespace's event emitter
                     */
                    cleanup: () => void;
                    clients: Set<EventBrokerClient>;
                };
            };
        };
    } = {};

    const subscribe = (namespaceName: string, keyName: string, client: EventBrokerClient) => {
        namespaceMap[namespaceName] ??= {
            eventEmitter: new TypedEmitter(),
            keyClientMap: {},
        };

        const definition = namespaceMap[namespaceName];

        // If there aren't any subscribers on the key yet, the keyClientMap won't be present
        if (!definition.keyClientMap[keyName]) {
            const clients = new Set<EventBrokerClient>();

            // Single listener forwards all events on this key to all subscribers
            const keyEventsListener = (event: EventInfo) => clients.forEach((forwardEvent) => forwardEvent(event));

            definition.keyClientMap[keyName] = {
                cleanup: () => void definition.eventEmitter.off(keyName, keyEventsListener),
                clients,
            };

            definition.eventEmitter.on(keyName, keyEventsListener);
        }

        definition.keyClientMap[keyName].clients.add(client);

        // Unsubscribe
        return () => {
            // If the client is the last one listening on the key, clean up memory
            if (definition.keyClientMap[keyName]!.clients.size == 1) {
                definition.keyClientMap[keyName]!.cleanup();
                delete definition.keyClientMap[keyName];
                return;
            }

            definition.keyClientMap[keyName]!.clients.delete(client);
        };
    };

    const publish = (namespaceName: string, keyName: string, event: EventInfo) => {
        const definition = namespaceMap[namespaceName];
        if (!definition) return;

        // If there are no clients listening on the key, do nothing
        if (!definition.keyClientMap[keyName]?.clients?.size) return;

        definition.eventEmitter.emit(keyName, event);
    };

    return {
        subscribe,
        publish,
    };
};

type RedisEventBrokerOptions = {
    getChannelName?: (namespaceName: string, keyName: string) => string;
    options: RedisClientOptions;
};

export const redisEventBroker = ({
    getChannelName = (namespaceName, keyName) => `${namespaceName}_${keyName}`,
    options,
}: RedisEventBrokerOptions): EventBroker & { initialize: () => Promise<void> } => {
    const clients: {
        pubClient: RedisClientType | null;
        subClient: RedisClientType | null;
    } = {
        pubClient: null,
        subClient: null,
    };

    const initialize = async () => {
        const [pubClient, subClient] = await Promise.all([createClient(options).connect(), createClient(options).connect()]);

        clients.pubClient = pubClient as RedisClientType;
        clients.subClient = subClient as RedisClientType;
    };

    const subscribe = async (namespaceName: string, keyName: string, client: EventBrokerClient) => {
        assert(clients.subClient, 'Redis Event Broker not initialized.');

        const listener = (message: string) => {
            const event = JSON.parse(message) as EventInfo;
            client(event);
        };

        const channelName = getChannelName(namespaceName, keyName);

        await clients.subClient!.subscribe(channelName, listener);

        return () => clients.subClient!.unsubscribe(channelName, listener);
    };

    const publish = async (namespaceName: string, keyName: string, event: EventInfo) => {
        assert(clients.pubClient, 'Redis Event Broker not initialized.');

        await clients.pubClient!.publish(getChannelName(namespaceName, keyName), JSON.stringify(event));
    };

    return {
        initialize,
        subscribe,
        publish,
    };
};

/**
 * Delivers events to `res` whenever the batch lifespan or size has exceeded.
 */
const eventBatch = (
    maxBatchSize: number,
    maxBatchLifespanMilliseconds: number,
    reconnectionTimeMilliseconds: number,
    res: ServerResponse
) => {
    const batch: EventInfo[] = [];

    const sendBatch = () =>
        res.write(
            batch
                .splice(0, maxBatchSize)
                .map((event) => `id:${event.id}\nevent:${event.event}\ndata:${event.data}\nretry:${reconnectionTimeMilliseconds}\n\n`)
                .join('')
        );

    let timeout: NodeJS.Timeout;

    return {
        add: (event: EventInfo) => {
            if (batch.length == 0) {
                timeout = setTimeout(sendBatch, maxBatchLifespanMilliseconds);
            }

            batch.push(event);

            if (batch.length >= maxBatchSize) {
                clearTimeout(timeout);
                sendBatch();
            }
        },
        close: () => {
            batch.splice(0, maxBatchSize);
            clearTimeout(timeout);
        },
    };
};

type NamespaceAuthConfig = {
    setCookiesForSubscriber?: (keyName: string, req: IncomingMessage) => Awaitable<string[]>;
    authorizePublisher?: (keyName: string, req: IncomingMessage) => Awaitable<boolean>;
    authorizeSubscriber?: (keyName: string, req: IncomingMessage) => Awaitable<boolean>;
};

type NamespaceDefinition = {
    events: {
        [keyName: string]: {
            validateData?: (eventData: string) => boolean;
            enrichData?: (eventData: string, req: IncomingMessage) => string;
        };
    };
    auth: NamespaceAuthConfig;
};

type PubSubOptions = {
    eventBroker?: EventBroker;
    reconnectionTimeMilliseconds?: number;
    maxBatchSize?: number;
    maxBatchLifespanMilliseconds?: number;
};

const enum Fault {
    Client = 'client',
    Server = 'server',
}

const jsonError = (fault: Fault, message: string, statusCode: number) => (res: ServerResponse) => {
    res.setHeader('Content-Type', 'application/json');
    res.writeHead(statusCode);

    res.write(JSON.stringify({ fault, message }));
    res.end();
};

const pubSub = ({
    eventBroker = inMemoryEventBroker(),
    reconnectionTimeMilliseconds = 3_000,
    maxBatchSize = 5,
    maxBatchLifespanMilliseconds = 100,
}: PubSubOptions) => {
    const namespaces: {
        [namespaceName: string]: NamespaceDefinition;
    } = {};

    const handleGet = async (namespaceName: string, keyName: string, req: IncomingMessage, res: ServerResponse) => {
        try {
            const { authorizeSubscriber, setCookiesForSubscriber } = namespaces[namespaceName]!.auth;

            if (authorizeSubscriber && !(await authorizeSubscriber(keyName, req))) {
                jsonError(Fault.Client, 'Unauthorized.', 401)(res);
                return;
            }

            if (setCookiesForSubscriber) {
                const cookies = await setCookiesForSubscriber(keyName, req);
                if (cookies.length) cookies.forEach((cookie) => res.appendHeader('Set-Cookie', cookie));
            }

            res.setHeader('Content-Type', 'text/event-stream');
            res.setHeader('Cache-Control', 'no-cache');
            res.setHeader('Connection', 'keep-alive');

            res.writeHead(200);
            // Flush
            res.write('');

            const batch = eventBatch(maxBatchSize, maxBatchLifespanMilliseconds, reconnectionTimeMilliseconds, res);

            const unsubscribe = await eventBroker.subscribe(namespaceName, keyName, (event) => batch.add(event));

            req.once('close', () => {
                res.end();
                unsubscribe();
                batch.close();
            });
        } catch (err) {
            jsonError(Fault.Server, 'Internal server error.', 500)(res);
        }
    };

    const handlePost = async (namespaceName: string, keyName: string, req: IncomingMessage, res: ServerResponse) => {
        try {
            const { authorizePublisher } = namespaces[namespaceName]!.auth;

            if (authorizePublisher && !(await authorizePublisher(keyName, req))) {
                jsonError(Fault.Client, 'Unauthorized.', 401)(res);
                return;
            }

            const chunks: Buffer[] = [];
            req.on('data', (chunk: Buffer) => chunks.push(chunk));
            const body = (await new Promise((resolve) =>
                req.once('end', () => resolve(Buffer.concat(chunks).toString()))
            )) satisfies string;

            const event = JSON.parse(body) as EventInfo;
            if (typeof event?.data !== 'string') {
                jsonError(Fault.Client, 'Event data must be a string.', 400)(res);
                return;
            }

            if (!event?.event) {
                jsonError(Fault.Client, 'Cannot use an empty event name.', 400)(res);
                return;
            }

            if (event.event === '*') {
                jsonError(Fault.Client, 'Cannot send an event using wildcard name.', 400)(res);
                return;
            }

            const eventDefinition = namespaces[namespaceName]!.events[event.event] || namespaces[namespaceName]!.events['*'];
            if (!eventDefinition) {
                jsonError(Fault.Client, `"${event.event}" is not a valid event type on "${namespaceName}".`, 400)(res);
                return;
            }

            const validateFn = eventDefinition.validateData;
            if (validateFn && !validateFn(event.data)) {
                jsonError(Fault.Client, 'Invalid event data.', 400)(res);
                return;
            }

            const enrichData = eventDefinition.enrichData;
            if (enrichData) event.data = enrichData(event.data, req);

            event.id = nanoid();

            await eventBroker.publish(namespaceName, keyName, event);

            res.writeHead(202);
            res.end();
        } catch (err) {
            jsonError(Fault.Server, 'Internal server error.', 500)(res);
        }
    };

    return {
        namespace: (namespaceName: string) => {
            namespaces[namespaceName] ??= {
                events: {},
                auth: {},
            };

            return {
                allowEventType: (
                    eventName: string,
                    options: {
                        validateData?: (eventData: string) => boolean;
                        enrichData?: (eventData: string, req: IncomingMessage) => string;
                    } = {}
                ) => {
                    namespaces[namespaceName]!.events[eventName] = options;
                },
                configureAuth: (auth: NamespaceAuthConfig) => {
                    namespaces[namespaceName]!.auth = auth;
                },
            };
        },
        publish: (namespaceName: string, keyName: string, event: Omit<EventInfo, 'id'>) =>
            eventBroker.publish(namespaceName, keyName, {
                ...event,
                id: nanoid(),
            }),
        handler: async (req: IncomingMessage, res: ServerResponse) => {
            const searchParams = new URL(req.url!, 'https://example.com').searchParams;
            const namespaceName = searchParams.get('namespace');
            const keyName = searchParams.get('key');

            if (!namespaceName || !keyName) {
                jsonError(Fault.Client, 'Invalid namespace or key', 400)(res);
                return;
            }

            if (!namespaces[namespaceName]) {
                jsonError(Fault.Client, `Namespace "${namespaceName}" not found.`, 404)(res);
                return;
            }

            switch (req.method) {
                default:
                    jsonError(Fault.Client, 'Unsupported request method. Supported methods are GET and POST.', 400)(res);
                    break;
                case 'GET':
                    handleGet(namespaceName, keyName, req, res);
                    break;
                case 'POST':
                    handlePost(namespaceName, keyName, req, res);
                    break;
            }
        },
    };
};

export default pubSub;
