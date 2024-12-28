type Awaitable<T> = T | Promise<T>;

enum QueryParam {
    Namespace = 'namespace',
    Key = 'key',
}

const RESERVED_QUERY_PARAMETERS: string[] = Object.values(QueryParam);

const getFinalUrl = (url: URL, query?: Record<string, string>) => {
    if (!query) return url;

    const reservedParameter = Object.keys(query).find((key) => RESERVED_QUERY_PARAMETERS.includes(key));
    if (reservedParameter) throw new Error(`${reservedParameter} is a reserved parameter.`);

    const finalUrl = new URL(url);
    for (const key in query) finalUrl.searchParams.set(key, query[key]!);
    return finalUrl;
};

export type PubSubClientConfig = {
    baseUrl: string;
    path: string;
    credentials?: RequestCredentials;
};

export type NamespaceEventTypeMap = {
    [namespaceName: string]: string;
};

export type EventInfo = {
    id: string;
    event: string;
    data: string;
};

export type Subscriber<EventTypeUnion = string> = {
    connected: boolean;
    onError: (handler: () => Awaitable<void>) => void;
    onConnect: (handler: () => Awaitable<void>) => void;
    on: (eventName: EventTypeUnion, handler: (data: string) => Awaitable<void>) => () => void;
    unsubscribe: () => void;
};

const subscriber =
    <EventTypeUnion = string>(url: URL, credentials: RequestCredentials) =>
    (query?: Record<string, string>): Subscriber<EventTypeUnion> => {
        const finalUrl = getFinalUrl(url, query);

        const eventSource = new EventSource(finalUrl, {
            withCredentials: credentials !== 'omit',
        });

        return {
            get connected() {
                return eventSource.OPEN === 1;
            },
            onError: (handler: () => Awaitable<void>) => {
                eventSource.onerror = () => handler();
            },
            onConnect: (handler: () => Awaitable<void>) => {
                eventSource.onopen = () => handler();
            },
            on: (eventName: EventTypeUnion, handler: (data: string) => Awaitable<void>) => {
                const listener = ({ data }: MessageEvent<string>) => handler(data);

                eventSource.addEventListener(eventName as string, listener);

                return () => eventSource.removeEventListener(eventName as string, listener);
            },
            unsubscribe: () => eventSource.close(),
        };
    };

export type Publisher<EventTypeUnion = string> = (eventName: EventTypeUnion, data: string, query?: Record<string, string>) => Promise<void>;

const publisher =
    <EventTypeUnion = string>(url: URL, credentials: RequestCredentials): Publisher<EventTypeUnion> =>
    async (eventName: EventTypeUnion, data: string, query?: Record<string, string>) => {
        const finalUrl = getFinalUrl(url, query);

        const res = await fetch(finalUrl, {
            method: 'POST',
            body: JSON.stringify({
                event: eventName,
                data,
            }),
            credentials,
        });

        if (res.status !== 202) throw new Error('Failed to send event.');
    };

const namespace =
    <EventTypeMap = NamespaceEventTypeMap>(baseUrl: string, path: string, credentials: RequestCredentials) =>
    <K extends keyof EventTypeMap>(namespaceName: K) => ({
        /**
         * **Keys** are identifiers for channels within a namespace. When subscribed to a key, a client will receive all events sent on that key.
         * An example of a **key** name could be "chatroom-49230582", or "user-23490982-notifs"
         */
        key: (keyName: string) => {
            const url = new URL(path, baseUrl);
            url.searchParams.set('namespace', namespaceName as string);
            url.searchParams.set('key', keyName);

            return {
                subscribe: subscriber<EventTypeMap[K]>(url, credentials),
                publish: publisher<EventTypeMap[K]>(url, credentials),
            };
        },
    });

/**
 * Initialize a client to integrate with a Streamlit server.
 *
 * Provides an interface for subscribing to & publishing events on **namespace** channels, also called **keys**.
 *
 * **Namespaces** are logical separations of channels, each allowing a certain set of event types.
 * An example of a **namespace** name could be "main", "chatrooms", or "notifications".
 *
 * **Keys** are identifiers for channels within a namespace. When subscribed to a key, a client will receive all events sent on that key.
 * An example of a **key** name could be "chatroom-49230582", or "user-23490982-notifs"
 *
 * @example
 * const client = pubSubClient({ baseUrl: 'https://localhost:8000', path: '/events' });
 *
 * const notifications = client.namespace('notifications');
 *
 * const weatherData = notifications.key('weather-data');
 *
 * const subscriber = weatherData.subscribe();
 * subscriber.on('update', (data) => {
 *     console.log(data);
 * });
 *
 * const userEvents = client.namespace('')
 */
export const pubSubClient = <EventTypeMap = NamespaceEventTypeMap>({ baseUrl, path, credentials = 'omit' }: PubSubClientConfig) => {
    if (!baseUrl) throw new Error('Must provide a base URL.');

    return {
        namespace: namespace<EventTypeMap>(baseUrl, path, credentials),
    };
};
