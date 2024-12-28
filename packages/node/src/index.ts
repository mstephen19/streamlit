import EventEmitter from 'events';
import { nanoid } from 'nanoid';
import { createClient, RedisClientType, type RedisClientOptions } from 'redis';
import assert from 'assert';

import type { IncomingMessage, ServerResponse } from 'http';
import type { TypedEmitter } from 'tiny-typed-emitter';

type Awaitable<T> = T | Promise<T>;

type EventInfo = {
    id: string;
    event: string;
    data: string;
};

const eventToString = (event: EventInfo, reconnectionTimeMilliseconds: number) =>
    `id:${event.id}\nevent:${event.event}\ndata:${event.data}\nretry:${reconnectionTimeMilliseconds}\n\n`;

const INITIAL_EVENT_NAME = '__init__';

// Sends a server-sent-event to the client
export type EventBrokerClient = (event: EventInfo) => void;

/**
 * A generic interface for subscribing & publishing events.
 */
export type EventBroker = {
    /**
     * Create a subscriber for a given key in a namespace. One subscriber is created per client.
     *
     * Called by PubSub when a GET request is received, and sends server-sent-events each time a message is received
     * via the `client()` callback.
     */
    subscribe: (namespaceName: string, keyName: string, client: EventBrokerClient) => Awaitable<() => Awaitable<void>>;
    /**
     * Publish an event to all subscribers listening on a given key in a namespace.
     *
     * Called by PubSub when a POST request is received.
     */
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
            eventEmitter: new EventEmitter() as TypedEmitter<{
                [keyName: string]: (event: EventInfo) => void;
            }>,
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

/**
 * A light wrapper around Redis pub-sub, implementing {@link EventBroker} for support with SSE PubSub.
 */
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
                .map((event) => eventToString(event, reconnectionTimeMilliseconds))
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

export type NamespaceAuthConfig = {
    /**
     * Set a cookie each time a client subscribes.
     */
    setCookiesForSubscriber?: (keyName: string, req: IncomingMessage) => Awaitable<string[]>;
    /**
     * Validate a publisher via cookies, headers, or any other means.
     *
     * Returning `false` results in an "Unauthorized" error for the client.
     */
    authorizePublisher?: (keyName: string, req: IncomingMessage) => Awaitable<boolean>;
    /**
     * Validate a subscriber via cookies, headers, or any other means.
     *
     * Returning `false` results in an "Unauthorized" error for the client.
     */
    authorizeSubscriber?: (keyName: string, req: IncomingMessage) => Awaitable<boolean>;
};

type EventDefinition = {
    /**
     * Validate event data (e.g. preventing empty strings from being sent).
     */
    validateData?: (eventData: string) => boolean;
    /**
     * Modify the event data before it is either forwarded to other clients, or captured (if `captureEvent()` is present).
     */
    enrichData?: (eventData: string, req: IncomingMessage) => string;
    /**
     * Prevent an event type from being forwarded to all clients subscribed in the keyspace, and instead handle the event on the server.
     */
    captureEvent?: (event: EventInfo, req: IncomingMessage) => Awaitable<void>;
};

type NamespaceDefinition = {
    events: {
        [keyName: string]: EventDefinition;
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
    maxBatchSize = 0,
    maxBatchLifespanMilliseconds = 0,
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
            res.write(`id:\nevent:${INITIAL_EVENT_NAME}\ndata:\nretry:${reconnectionTimeMilliseconds}\n\n`);

            const shouldBatch = maxBatchLifespanMilliseconds > 0 && reconnectionTimeMilliseconds > 0;

            const batch = shouldBatch ? eventBatch(maxBatchSize, maxBatchLifespanMilliseconds, reconnectionTimeMilliseconds, res) : null;

            const unsubscribe = await eventBroker.subscribe(
                namespaceName,
                keyName,
                batch ? (event) => batch.add(event) : (event) => res.write(eventToString(event, reconnectionTimeMilliseconds))
            );

            req.once('close', () => {
                res.end();
                unsubscribe();
                batch?.close();
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

            if (event.event === '*' || event.event === INITIAL_EVENT_NAME) {
                jsonError(Fault.Client, 'Forbidden event name.', 404)(res);
                return;
            }

            const eventDefinition = namespaces[namespaceName]!.events[event.event] || namespaces[namespaceName]!.events['*'];
            if (!eventDefinition) {
                jsonError(Fault.Client, `"${event.event}" is not a valid event type on "${namespaceName}".`, 400)(res);
                return;
            }

            const validateFn = eventDefinition.validateData;
            if (validateFn && !validateFn(event.data)) {
                jsonError(Fault.Client, 'Invalid event data.', 406)(res);
                return;
            }

            const enrichData = eventDefinition.enrichData;
            if (enrichData) event.data = enrichData(event.data, req);

            event.id = nanoid();

            const captureEvent = eventDefinition.captureEvent;
            if (captureEvent) captureEvent(event, req);
            else eventBroker.publish(namespaceName, keyName, event);

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
                /**
                 * Add to the list of events allowed to be published from the client-side.
                 *
                 * Only the event types you specify are allowed to be sent on the namespace by clients, unless a type of **\*** is specified.
                 */
                allowEventType: (eventName: string, options: EventDefinition = {}) => {
                    namespaces[namespaceName]!.events[eventName] = options;
                },
                /**
                 * Apply {@link NamespaceAuthConfig} to this namespace, allowing fine-grained control over which clients are allowed to
                 * subscribe & publish to keys on the namespace.
                 */
                configureAuth: (auth: NamespaceAuthConfig) => {
                    namespaces[namespaceName]!.auth = auth;
                },
            };
        },
        /**
         * Publishes an event from the server to all subscribers listening on a given key in a namespace.
         *
         * Skips event type & data validation. In other words, the server is allowed to send any type of event, even if it wasn't registered with `allowEventType()`.
         */
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
                case 'OPTIONS':
                    res.writeHead(204);
                    res.end();
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
