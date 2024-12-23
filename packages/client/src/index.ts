type Awaitable<T> = T | Promise<T>;

export type PubSubClientConfig = {
    baseUrl: string;
    path: string;
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

export type Publisher<EventTypeUnion = string> = (eventName: EventTypeUnion, data: string) => Promise<void>;

const subscriber =
    <EventTypeUnion = string>(url: URL) =>
    (options?: EventSourceInit): Subscriber<EventTypeUnion> => {
        const eventSource = new EventSource(url, options);

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

const publisher =
    <EventTypeUnion = string>(url: URL): Publisher<EventTypeUnion> =>
    async (eventName: EventTypeUnion, data: string) => {
        const res = await fetch(url, {
            method: 'POST',
            body: JSON.stringify({
                event: eventName,
                data,
            }),
        });

        if (res.status !== 202) throw new Error('Failed to send event.');
    };

const namespace =
    <EventTypeMap = NamespaceEventTypeMap>(baseUrl: string, path: string) =>
    <K extends keyof EventTypeMap>(namespaceName: K) => ({
        key: (keyName: string) => {
            const url = new URL(path, baseUrl);
            url.searchParams.set('namespace', namespaceName as string);
            url.searchParams.set('key', keyName);

            return {
                subscribe: subscriber<EventTypeMap[K]>(url),
                publish: publisher<EventTypeMap[K]>(url),
            };
        },
    });

export const pubSubClient = <EventTypeMap = NamespaceEventTypeMap>({ baseUrl, path }: PubSubClientConfig) => {
    if (!baseUrl) throw new Error('Must provide a base URL.');

    return {
        namespace: namespace<EventTypeMap>(baseUrl, path),
    };
};
