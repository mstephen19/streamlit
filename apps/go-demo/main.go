package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	streamlit "github.com/mstephen19/streamlit"
	"github.com/redis/go-redis/v9"
)

type Message struct {
	Sender  string `json:"sender"`
	Content string `json:"content"`
}

func main() {
	pubSub := streamlit.NewPubSub(streamlit.PubSubConfig{
		// Choosing to use Redis under the hood.
		// Can be replaced by anything that implements the EventBroker interface
		EventBroker: &streamlit.RedisEventBroker{
			Options: &redis.Options{
				Addr:     "localhost:6379",
				Password: "",
				DB:       0,
			},
		},
	})

	chats := pubSub.Namespace("chats")

	auth := &streamlit.NamespaceAuth{}
	auth.AuthorizeSubscriber(func(keyName string, r *http.Request) bool {
		nickname := r.URL.Query().Get("nickname")

		return nickname != ""
	})

	// When a client subscribes, store a cookie indicating that they're listening on a given key
	// using a given nickname
	auth.SetSubscriberCookies(func(keyName string, r *http.Request) ([]*http.Cookie, bool) {
		nickname := r.URL.Query().Get("nickname")

		return []*http.Cookie{
			{
				Name:     "_session",
				Value:    fmt.Sprintf("%s__%s", keyName, nickname),
				MaxAge:   int(time.Hour.Seconds()),
				Secure:   true,
				HttpOnly: true,
				SameSite: http.SameSiteNoneMode,
			},
		}, true
	})

	// Ensure only clients who are subscribed to a key can publish to it
	auth.AuthorizePublisher(func(keyName string, r *http.Request) bool {
		sessionCookie, err := r.Cookie("_session")
		if err != nil {
			return false
		}

		split := strings.Split(sessionCookie.Value, "__")
		if len(split) != 2 {
			return false
		}

		return keyName == split[0] && split[1] != ""
	})

	chats.ConfigureAuth(auth)

	// Allow "message" events to be sent front the client, as long as they contain
	// a non-empty "content" property.
	// Add the sender's nickname to the message data
	chats.AllowEventType("message").Validate(func(data string, r *http.Request) (bool, string) {
		message := &Message{}
		json.Unmarshal([]byte(data), message)

		if message.Content == "" {
			return false, ""
		}

		sessionCookie, err := r.Cookie("_session")
		if err != nil {
			return false, ""
		}

		split := strings.Split(sessionCookie.Value, "__")

		message.Sender = split[1]

		enrichedMessage, err := json.Marshal(message)
		if err != nil {
			return false, ""
		}

		return true, string(enrichedMessage)
	})

	serveMux := &http.ServeMux{}

	serveMux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		// For demo purposes only
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:5173")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Cookie")

		if r.Method == http.MethodOptions {
			w.WriteHeader(204)
			return
		}

		pubSub.ServeHTTP(w, r)
	})

	fmt.Println("TLS server listening on port 8000")
	log.Fatal(http.ListenAndServeTLS(":8000", "../../ssl/cert.pem", "../../ssl/cert.key", serveMux))
}
