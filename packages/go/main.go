// Concepts:
//
// Namespace:
// A mechanism allowing for the logical separation between channel types
// e.g. "Messages", "Notifications", "Changes"
//
// Key:
// A specific channel in a namespace, holding any number of clients
// e.g. "room1", "client-a6XydU", "document-2inUne"
// Keys within a namespace are created on demand, as-needed
//
// Event:
// A message with some data, sent from one client (or the server) to all
// clients listening on a certain key
// Every event has:
// - An ID
// - A type ("event")
// - Data (always a string)
package streamlit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/redis/go-redis/v9"
)

func randomId() string {
	id, _ := gonanoid.New()
	return id
}

type fault string

const (
	faultClient fault = "client"
	faultServer fault = "server"
)

type jsonError struct {
	statusCode int
	fault      `json:"fault"`
	Error      string `json:"error"`
}

func newJsonError() *jsonError {
	return &jsonError{}
}

func (errResponse *jsonError) client() *jsonError {
	errResponse.fault = faultClient
	return errResponse
}

func (errResponse *jsonError) server() *jsonError {
	errResponse.fault = faultServer
	return errResponse
}

func (errResponse *jsonError) message(message string) *jsonError {
	errResponse.Error = message
	return errResponse
}

func (errResponse *jsonError) status(statusCode int) *jsonError {
	errResponse.statusCode = statusCode
	return errResponse
}

func (errResponse *jsonError) send(w http.ResponseWriter) {
	bytes, err := json.Marshal(errResponse)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(errResponse.statusCode)

	if err != nil {
		w.Write([]byte(errResponse.Error))
		return
	}

	w.Write(bytes)
}

type ResponseWriterFlusher interface {
	http.ResponseWriter
	http.Flusher
}

// A standardized format for server-sent events.
type Event struct {
	// A unique ID for the event
	Id string `json:"id"`
	// The "type" of event
	Event string `json:"event"`
	// Raw event data in string form
	Data string `json:"data"`
}

func (event *Event) toString(reconnectionTime time.Duration) string {
	return fmt.Sprintf("id:%s\nevent:%s\ndata:%s\nretry:%d\n\n", event.Id, event.Event, event.Data, reconnectionTime.Milliseconds())
}

type eventBatch []*Event

func (batch eventBatch) toString(reconnectionTime time.Duration) string {
	builder := strings.Builder{}

	for _, event := range batch {
		builder.WriteString(event.toString(reconnectionTime))
	}

	return builder.String()
}

type NamespaceAuth struct {
	// When a user subscribes, the cookie returned by this function will be set
	getCookiesToSetForSubscriber func(keyName string, r *http.Request) ([]*http.Cookie, bool)
	// Check if a publisher is authorized (e.g. based on cookie)
	validatePublisher func(keyName string, r *http.Request) bool
	// Check if a subscriber is authorized (e.g. based on cookie)
	validateSubscriber func(keyName string, r *http.Request) bool
}

// Set a cookie each time a client subscribes.
func (auth *NamespaceAuth) SetSubscriberCookies(getCookiesToSetForSubscriber func(keyName string, r *http.Request) ([]*http.Cookie, bool)) {
	auth.getCookiesToSetForSubscriber = getCookiesToSetForSubscriber
}

// Validate a publisher via cookies, headers, or any other means.
//
// Returning false results in an "Unauthorized." error for the client.
func (auth *NamespaceAuth) AuthorizePublisher(validatePublisher func(keyName string, r *http.Request) bool) {
	auth.validatePublisher = validatePublisher
}

// Validate a subscriber via cookies, headers, or any other means.
//
// Returning false results in an "Unauthorized." error for the client.
func (auth *NamespaceAuth) AuthorizeSubscriber(validateSubscriber func(keyName string, r *http.Request) bool) {
	auth.validateSubscriber = validateSubscriber
}

// Key: Event Type
//
// Value: Function that validates & enriched event data
type namespaceEventMap map[string]func(data string, r *http.Request) (bool, string)

type namespaceDefinition struct {
	namespaceEventMap
	auth *NamespaceAuth
}

// Add to the list of events allowed to be published from the client-side.
//
// Validate event data (e.g. preventing empty strings from being sent) with a validator function, passed as the second argument.
// Passing nil skips event data validation.
//
// The validator function can also optionally modify the event data by including a non-empty string as its second return value.
// Pass an empty string ("") to skip enriching and maintain the original event data.
//
// Only the event types you specify are allowed to be sent on the namespace, unless a type of "*" is specified
func (definition namespaceDefinition) AllowEventType(eventType string, validateAndEnrichData func(data string, r *http.Request) (bool, string)) {
	definition.namespaceEventMap[eventType] = validateAndEnrichData
}

func (definition *namespaceDefinition) ConfigureAuth(auth *NamespaceAuth) {
	definition.auth = auth
}

// Key: Namespace name
type namespaceMap map[string]*namespaceDefinition

type EventBrokerSubscriber interface {
	// Cleans up anything related to the subscription, and closes the subscriber's channel.
	Unsubscribe()
	// Returns a channel that receives messages for the subscriber.
	Channel() <-chan *Event
}

// todo: RedisShardBroker
// A generic interface for subscribing & publishing events.
type EventBroker interface {
	// To be called before the EventBroker is used.
	Initialize()
	// Publish an event to all subscribers listening on a given key in a namespace.
	//
	// Called by PubSub when a POST request is received.
	Publish(namespaceName, keyName string, event *Event)
	// Create a subscriber for a given key in a namespace. One subscriber per client.
	//
	// Called by PubSub when a GET request is received, and sends server-sent-events each time a message is received on the subscriber's channel.
	Subscribe(namespaceName, keyName string) EventBrokerSubscriber
}

type syncMap[K comparable, V any] struct {
	data  map[K]V
	mutex *sync.RWMutex
}

func newSyncMap[K comparable, V any]() *syncMap[K, V] {
	return &syncMap[K, V]{
		data:  map[K]V{},
		mutex: &sync.RWMutex{},
	}
}

type inMemorySubscriber struct {
	namespaceName string
	keyName       string
	channel       chan *Event
	broker        *InMemoryEventBroker
}

func (subscriber *inMemorySubscriber) Channel() <-chan *Event {
	return subscriber.channel
}

func (subscriber *inMemorySubscriber) Unsubscribe() {
	// Drain & close the channel
	go func() {
		for range subscriber.channel {
		}
	}()
	close(subscriber.channel)

	// Map of Namespaces -> Keys -> Clients
	subscriber.broker.clients.mutex.RLock()
	keyClientMap := subscriber.broker.clients.data[subscriber.namespaceName]
	subscriber.broker.clients.mutex.RUnlock()

	// Map of Keys -> Clients
	keyClientMap.mutex.Lock()
	defer keyClientMap.mutex.Unlock()
	clientMap := keyClientMap.data[subscriber.keyName]

	// Map of Clients
	clientMap.mutex.Lock()
	defer clientMap.mutex.Unlock()

	// The key is dead if there are no subscribers. Clean it up.
	if len(clientMap.data) == 1 {
		delete(keyClientMap.data, subscriber.keyName)
		return
	}

	// Otherwise, remove this client
	delete(clientMap.data, subscriber.channel)
}

// Stores all clients in an in-memory map. For small projects with low traffic or demo purposes only.
//
// Uses a simple nested map data structure:
//
//	[namespaceName]:
//	    [keyName]:
//	        <-chan *Event
//	        <-chan *Event
//	    [keyName]:
//	        <-chan *Event
//	[namespaceName]:
//	    [keyName]:
//	        <-chan *Event
//	        <-chan *Event
//	        <-chan *Event
type InMemoryEventBroker struct {
	// Namespace -> Key -> Client -> Empty Struct
	clients *syncMap[string, *syncMap[string, *syncMap[chan *Event, struct{}]]]
}

func (broker *InMemoryEventBroker) Initialize() {
	broker.clients = newSyncMap[string, *syncMap[string, *syncMap[chan *Event, struct{}]]]()
}

func (broker *InMemoryEventBroker) Publish(namespaceName, keyName string, event *Event) {
	// Do nothing if the namespace doesn't exist
	broker.clients.mutex.RLock()
	keyClientMap, ok := broker.clients.data[namespaceName]
	broker.clients.mutex.RUnlock()
	if !ok {
		return
	}

	// Do nothing if there are no clients
	keyClientMap.mutex.RLock()
	clientMap, ok := keyClientMap.data[keyName]
	keyClientMap.mutex.RUnlock()
	if !ok || len(clientMap.data) == 0 {
		return
	}

	clientMap.mutex.RLock()
	defer clientMap.mutex.RUnlock()

	// Send the message to all clients
	for client := range clientMap.data {
		client <- event
	}
}

// Adds a new entry to the in-memory list of clients under the given key in the namespace.
func (broker *InMemoryEventBroker) Subscribe(namespaceName, keyName string) EventBrokerSubscriber {
	subscriber := &inMemorySubscriber{
		broker:        broker,
		namespaceName: namespaceName,
		keyName:       keyName,
		channel:       make(chan *Event),
	}

	// Add a new namespace entry if it doesn't already exist
	broker.clients.mutex.Lock()
	keyClientMap, ok := broker.clients.data[namespaceName]
	if !ok {
		keyClientMap = newSyncMap[string, *syncMap[chan *Event, struct{}]]()
		broker.clients.data[namespaceName] = keyClientMap
	}
	broker.clients.mutex.Unlock()

	// If no other clients are listening on the key yet, the client list won't be defined
	// and must be created.
	keyClientMap.mutex.Lock()
	clientMap, ok := keyClientMap.data[keyName]
	if !ok {
		clientMap = newSyncMap[chan *Event, struct{}]()
		keyClientMap.data[keyName] = clientMap
	}
	keyClientMap.mutex.Unlock()

	// Add the channel to the list of subscriber clients
	clientMap.mutex.Lock()
	defer clientMap.mutex.Unlock()
	clientMap.data[subscriber.channel] = struct{}{}

	return subscriber
}

type redisSubscriber struct {
	redisPubSub *redis.PubSub
	channel     <-chan *Event
}

func (subscriber *redisSubscriber) Unsubscribe() {
	subscriber.redisPubSub.Close()
}

func (subscriber *redisSubscriber) Channel() <-chan *Event {
	return subscriber.channel
}

// A light wrapper around Redis pub-sub, implementing EventBroker for support with SSE PubSub.
type RedisEventBroker struct {
	// An optional function to generate a channel name based on a namespace name & key.
	GetChannelName func(namespaceName, keyName string) string
	Options        *redis.Options
	// rdb            *redis.Client
	pubClient *redis.Client
	subClient *redis.Client
}

func connectRedis(options *redis.Options) (client *redis.Client) {
	client = redis.NewClient(options)

	cmd := client.Ping(context.Background())

	if cmd.Val() != "PONG" {
		log.Fatal(errors.New("failed to connect to Redis"))
	}

	return
}

func (broker *RedisEventBroker) Initialize() {
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		broker.pubClient = connectRedis(broker.Options)
		wg.Done()
	}()

	go func() {
		broker.subClient = connectRedis(broker.Options)
		wg.Done()
	}()

	if broker.GetChannelName == nil {
		broker.GetChannelName = func(namespaceName, keyName string) string {
			return fmt.Sprintf("%s_%s", namespaceName, keyName)
		}
	}

	wg.Wait()
}

// Converts all messages received on a Redis pub-sub subscriber channel to *Event, and delivers them on a new channel.
func wrapRedisPubSubChannel(in <-chan *redis.Message) <-chan *Event {
	out := make(chan *Event)

	go func() {
		defer close(out)

		for message := range in {
			event := &Event{}
			json.Unmarshal([]byte(message.Payload), event)

			out <- event
		}
	}()

	return out
}

func (broker *RedisEventBroker) Subscribe(namespaceName, keyName string) EventBrokerSubscriber {
	redisPubSub := broker.subClient.Subscribe(context.Background(), broker.GetChannelName(namespaceName, keyName))

	subscriber := &redisSubscriber{
		redisPubSub: redisPubSub,
		channel:     wrapRedisPubSubChannel(redisPubSub.Channel()),
	}

	return subscriber
}

func (broker *RedisEventBroker) Publish(namespaceName, keyName string, event *Event) {
	if event == nil {
		return
	}

	rawEvent, err := json.Marshal(event)
	if err != nil {
		return
	}

	broker.pubClient.Publish(context.Background(), broker.GetChannelName(namespaceName, keyName), rawEvent)
}

// A pub-sub provider based on server-sent-events & your chosen event broker (e.g. Redis, AWS SNS, Apache Kafka).
//
// Allows for bi-directional communication between the client & server, in addition to client-to-client communication.
//
// Use NewPubSub() to initialize.
type PubSub struct {
	namespaceMap
	eventBroker      EventBroker
	maxBatchSize     int
	maxBatchLifespan time.Duration
	reconnectionTime time.Duration
}

type PubSubConfig struct {
	// Maximum number of events that can be batched together.
	MaxBatchSize int
	// Maximum amount of time until an event batch must be sent to the client (even if batch size is 1).
	MaxBatchLifespan time.Duration
	// Amount of time for subscribers to wait before attempting reconnection, if disconnection occurs without the subscriber unsubscribing.
	ReconnectionTime time.Duration
	// An instance of the EventBroker implementation supporting your chosen service (e.g. Redis, AWS SNS, Apache Kafka).
	EventBroker
}

// Create a pub-sub provider based on server-sent-events & your chosen event broker (e.g. Redis, AWS SNS, Apache Kafka).
//
// PubSub allows for bi-directional communication between the client & server, in addition to client-to-client communication.
func NewPubSub(config PubSubConfig) *PubSub {
	if config.EventBroker == nil {
		config.EventBroker = &InMemoryEventBroker{}
	}

	if config.MaxBatchSize == 0 {
		config.MaxBatchSize = 5
	}

	if config.MaxBatchLifespan == 0 {
		config.MaxBatchLifespan = time.Millisecond * 100
	}

	if config.ReconnectionTime == 0 {
		config.ReconnectionTime = time.Second * 3
	}

	pubSub := &PubSub{
		namespaceMap:     make(namespaceMap),
		eventBroker:      config.EventBroker,
		maxBatchSize:     config.MaxBatchSize,
		maxBatchLifespan: config.MaxBatchLifespan,
	}

	pubSub.eventBroker.Initialize()

	return pubSub
}

// Grabs the reference to a given namespace. Creates a new namespace if it doesn't already exist.
func (pubSub *PubSub) Namespace(namespaceName string) *namespaceDefinition {
	definition, ok := pubSub.namespaceMap[namespaceName]
	if !ok {
		definition = &namespaceDefinition{
			namespaceEventMap: make(namespaceEventMap),
		}
		pubSub.namespaceMap[namespaceName] = definition
	}

	return definition
}

// Publishes an event to all subscribers listening on a given key in a namespace.
//
// Skips event type & data validation. In other words, the server is allowed to send any type of event, even if it wasn't registered with RegisterEventType().
func (pubSub *PubSub) Publish(namespaceName, keyName string, event *Event) {
	event.Id = randomId()

	pubSub.eventBroker.Publish(namespaceName, keyName, event)
}

func (pubSub *PubSub) handleGet(namespaceName, keyName string, w ResponseWriterFlusher, r *http.Request) {
	authProvider := pubSub.namespaceMap[namespaceName].auth

	if authProvider != nil {
		// Authorize subscriber
		validateSubscriber := authProvider.validateSubscriber

		subscriberIsValid := validateSubscriber == nil || validateSubscriber(keyName, r)
		if !subscriberIsValid {
			newJsonError().client().message("Unauthorized.").status(http.StatusUnauthorized).send(w)
			return
		}

		// Set cookies for subscriber
		getCookies := authProvider.getCookiesToSetForSubscriber

		if getCookies != nil {
			cookies, ok := getCookies(keyName, r)
			if !ok {
				newJsonError().client().message("Failed to authorize.").status(http.StatusUnauthorized).send(w)
				return
			}

			if len(cookies) != 0 {
				for _, cookie := range cookies {
					http.SetCookie(w, cookie)
				}
			}
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	w.WriteHeader(http.StatusOK)

	w.Flush()

	subscriber := pubSub.eventBroker.Subscribe(namespaceName, keyName)

	batch := make(eventBatch, 0, pubSub.maxBatchSize)
	ticker := time.NewTicker(pubSub.maxBatchLifespan)

	sendBatch := func() {
		w.Write([]byte(batch.toString(pubSub.reconnectionTime)))
		batch = batch[:0]
		w.Flush()
	}

	defer func() {
		subscriber.Unsubscribe()
		ticker.Stop()
	}()

	// Blocks until the request closes
stream:
	for {
		select {
		// Events flow through until the request is closed
		case <-r.Context().Done():
			break stream
		case event, ok := <-subscriber.Channel():
			if ok && event != nil {
				batch = append(batch, event)
			}

			// Once the buffer max size is reached, flush all events regardless of time
			if len(batch) >= pubSub.maxBatchSize {
				sendBatch()
			}
		// Once the buffer time is reached, flush all events regardless of batch size
		case <-ticker.C:
			if len(batch) > 0 {
				sendBatch()
			}
		}
	}
}

func (pubSub *PubSub) handlePost(namespaceName, keyName string, w http.ResponseWriter, r *http.Request) {
	authProvider := pubSub.namespaceMap[namespaceName].auth

	if authProvider != nil {
		validatePublisher := authProvider.validatePublisher

		publisherIsValid := validatePublisher == nil || validatePublisher(keyName, r)
		if !publisherIsValid {
			newJsonError().client().message("Unauthorized.").status(http.StatusUnauthorized).send(w)
			return
		}
	}

	rawBody, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		newJsonError().server().message("Failed to parse request body.").status(http.StatusInternalServerError).send(w)
		return
	}

	event := &Event{}
	err = json.Unmarshal(rawBody, event)
	if err != nil {
		newJsonError().client().message("Invalid request body.").status(http.StatusBadRequest).send(w)
		return
	}

	if event.Event == "" {
		newJsonError().client().message("Cannot use an empty event name.").status(http.StatusBadRequest).send(w)
		return
	}

	if event.Event == "*" {
		newJsonError().client().message("Cannot send an event using wildcard name.").status(http.StatusBadRequest).send(w)
		return

	}

	// Ensure the event type is valid
	validateAndEnrichData, ok := pubSub.namespaceMap[namespaceName].namespaceEventMap[event.Event]
	if !ok {
		// Allow all events to flow through if "*" is registered as an event type
		validateAndEnrichData, ok = pubSub.namespaceMap[namespaceName].namespaceEventMap["*"]
		if !ok {
			newJsonError().client().message(fmt.Sprintf("\"%s\" is not a valid event type on \"%s\".", event.Event, namespaceName)).status(http.StatusBadRequest).send(w)
			return
		}
	}

	if validateAndEnrichData != nil {
		dataIsValid, enrichedData := validateAndEnrichData(event.Data, r)
		if !dataIsValid {
			newJsonError().client().message("Invalid event data.").status(http.StatusBadRequest).send(w)
			return
		}

		if enrichedData != "" {
			event.Data = enrichedData
		}
	}

	event.Id = randomId()

	go pubSub.eventBroker.Publish(namespaceName, keyName, event)

	w.WriteHeader(http.StatusAccepted)
}

func (pubSub *PubSub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	namespaceName, keyName := r.URL.Query().Get("namespace"), r.URL.Query().Get("key")
	if namespaceName == "" || keyName == "" {
		newJsonError().client().message("Invalid namespace or key.").status(http.StatusBadRequest).send(w)
		return
	}

	// Ensure the namespace exists
	if _, ok := pubSub.namespaceMap[namespaceName]; !ok {
		newJsonError().client().message(fmt.Sprintf("Namespace \"%s\" not found.", namespaceName)).status(http.StatusNotFound).send(w)
		return
	}

	switch r.Method {
	default:
		{
			newJsonError().client().message("Unsupported request method. Supported methods are GET and POST.").status(http.StatusBadRequest).send(w)
			break
		}
	case http.MethodGet:
		{
			r.Body.Close()

			wf, ok := w.(ResponseWriterFlusher)
			if !ok {
				newJsonError().client().message("Internal server error.").status(http.StatusBadRequest).send(w)
				break
			}

			pubSub.handleGet(namespaceName, keyName, wf, r)

			break
		}

	case http.MethodPost:
		{
			pubSub.handlePost(namespaceName, keyName, w, r)

			break
		}
	}
}
