package main

import (
	// "encoding/json"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
		EventBroker: &streamlit.RedisEventBroker{
			Options: &redis.Options{
				Addr:     "localhost:6379",
				Password: "", // no password set
				DB:       0,  // use default DB
			},
		},
	})

	chatAuth := &streamlit.NamespaceAuth{}

	messages := pubSub.Namespace("messages")

	messages.AllowEventType("testmessage", func(data string, r *http.Request) (bool, string) {
		message := &Message{}
		json.Unmarshal([]byte(data), message)

		if message.Content == "" {
			return false, ""
		}

		return true, ""
	})

	messages.AllowEventType("testmessage2", func(data string, r *http.Request) (bool, string) {
		message := &Message{}
		json.Unmarshal([]byte(data), message)

		if message.Content == "" {
			return false, ""
		}

		return true, ""
	})

	go func() {
		ticker := time.NewTicker(time.Second)

		for range ticker.C {

		}
	}()

	messages.ConfigureAuth(chatAuth)

	serveMux := &http.ServeMux{}

	serveMux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		// For demo purposes only
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:5173")
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		pubSub.ServeHTTP(w, r)
	})

	fmt.Println("TLS server listening on port 8000")
	log.Fatal(http.ListenAndServeTLS(":8000", "../../ssl/cert.pem", "../../ssl/cert.key", serveMux))
}
