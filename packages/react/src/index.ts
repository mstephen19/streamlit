import { pubSubClient, type PubSubClientConfig, type NamespaceEventTypeMap, type Subscriber } from 'streamlit-client';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

type Awaitable<T> = T | Promise<T>;

type KeyspaceInfo<EventTypeMap, K extends keyof EventTypeMap> = {
    namespace: K;
    key: string;
};

const getSubscriberCacheKey = (key: string, query: Record<string, string> = {}) => {
    const params = new URLSearchParams(Object.entries(query).sort(([a], [b]) => a.localeCompare(b))).toString();

    return `${key}?${params}`;
};

export const pubSubHooks = <EventTypeMap extends NamespaceEventTypeMap = NamespaceEventTypeMap>(clientConfig: PubSubClientConfig) => {
    const subscriberCache: {
        [K in keyof EventTypeMap]?: {
            [keyName: string]: {
                count: number;
                subscriber: Subscriber<EventTypeMap[K]>;
            };
        };
    } = {};

    const client = pubSubClient<EventTypeMap>(clientConfig);

    const useKeyspace = <K extends keyof EventTypeMap>(namespaceName: K, keyName: string) =>
        useMemo(() => client.namespace(namespaceName).key(keyName), [namespaceName, keyName]);

    const usePublish = <K extends keyof EventTypeMap>({ namespace, key }: KeyspaceInfo<EventTypeMap, K>) => {
        const keyspace = useKeyspace(namespace, key);

        return keyspace.publish;
    };

    const useSubscribe = <K extends keyof EventTypeMap>({
        namespace,
        key,
        eventName,
        eventHandler,
        query,
    }: KeyspaceInfo<EventTypeMap, K> & {
        eventName: EventTypeMap[K];
        eventHandler: (data: string) => Awaitable<void>;
        query?: Record<string, string>;
    }) => {
        const [connected, setConnected] = useState(() => {
            return Boolean(subscriberCache[namespace]?.[key]?.subscriber?.connected);
        });
        const [error, setError] = useState(false);
        const keyspace = useKeyspace(namespace, key);

        const eventHandlerRef = useRef(eventHandler);

        useEffect(() => {
            eventHandlerRef.current = eventHandler;
        }, [eventHandler]);

        useEffect(() => {
            // Ensure cache accounts for query parameters as well
            // Different query parameters = different cache entry
            const cacheKey = getSubscriberCacheKey(key, query);

            setConnected(Boolean(subscriberCache[namespace]?.[cacheKey]?.subscriber?.connected));
            setError(false);

            // Create & cache a subscriber if not already present
            subscriberCache[namespace] ??= {};
            subscriberCache[namespace][cacheKey] ??= {
                subscriber: keyspace.subscribe(query),
                count: 0,
            };

            subscriberCache[namespace][cacheKey].count++;

            const subscriber = subscriberCache[namespace][cacheKey].subscriber;

            subscriber.onError(() => setError(true));
            subscriber.onConnect(() => {
                setConnected(true);
                setError(false);
            });

            const removeListener = subscriber.on(eventName, (data: string) => {
                eventHandlerRef.current(data);
            });

            return () => {
                removeListener();

                if (!subscriberCache[namespace]?.[cacheKey]) return;

                // If this component is the last one using the subscriber, disconnect
                if (subscriberCache[namespace][cacheKey].count === 1) {
                    subscriberCache[namespace][cacheKey].subscriber.unsubscribe();

                    delete subscriberCache[namespace][cacheKey];
                    return;
                }

                subscriberCache[namespace][cacheKey].count--;
            };
        }, [keyspace, eventName, query]);

        return {
            connected,
            error,
        };
    };

    return {
        usePublish,
        useSubscribe,
    };
};
