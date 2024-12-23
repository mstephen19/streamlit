import { pubSubClient, type PubSubClientConfig, type NamespaceEventTypeMap, type Subscriber } from 'streamlit-client';
import { useEffect, useMemo, useState } from 'react';

type Awaitable<T> = T | Promise<T>;

type KeyspaceInfo<EventTypeMap, K extends keyof EventTypeMap> = {
    namespace: K;
    key: string;
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

        return useMemo(() => keyspace.publish, [keyspace]);
    };

    const useSubscribe = <K extends keyof EventTypeMap>({
        namespace,
        key,
        eventName,
        eventHandler,
        withCredentials = false,
    }: KeyspaceInfo<EventTypeMap, K> & {
        eventName: EventTypeMap[K];
        eventHandler: (data: string) => Awaitable<void>;
        withCredentials?: boolean;
    }) => {
        const [connected, setConnected] = useState(() => {
            return Boolean(subscriberCache[namespace]?.[key]?.subscriber?.connected);
        });
        const [error, setError] = useState(false);
        const keyspace = useKeyspace(namespace, key);

        useEffect(() => {
            setConnected(Boolean(subscriberCache[namespace]?.[key]?.subscriber?.connected));
            setError(false);

            // Create & cache a subscriber if not already present
            subscriberCache[namespace] ??= {};
            subscriberCache[namespace][key] ??= {
                subscriber: keyspace.subscribe({ withCredentials }),
                count: 0,
            };

            subscriberCache[namespace][key].count++;

            const subscriber = subscriberCache[namespace][key].subscriber;

            subscriber.onError(() => setError(true));
            subscriber.onConnect(() => setConnected(true));

            const removeListener = subscriber.on(eventName, eventHandler);

            return () => {
                removeListener();

                if (!subscriberCache[namespace]?.[key]) return;

                // If this component is the last one using the subscriber, disconnect
                if (subscriberCache[namespace][key].count === 1) {
                    subscriberCache[namespace][key].subscriber.unsubscribe();

                    delete subscriberCache[namespace][key];
                    return;
                }

                subscriberCache[namespace][key].count--;
            };
        }, [keyspace, eventName, eventHandler, withCredentials]);

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
