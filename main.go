package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/handlers"
)

type Endpoint struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Name      string    `json:"name"`
	TargetURL string    `json:"target_url"`
	Provider  string    `json:"provider"`
	CreatedAt time.Time `json:"created_at"`
	IsActive  bool      `json:"is_active"`
}

type Event struct {
	ID         string    `json:"id"`
	EndpointID string    `json:"endpoint_id"`
	EventType  string    `json:"event_type"`
	Payload    any       `json:"payload"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
}

var endpoints = map[string]Endpoint{}
var events = []Event{}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "hookkeeper-api"})
}

func webhookHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	endpointID := vars["endpoint_id"]

	if _, ok := endpoints[endpointID]; !ok {
		endpoints[endpointID] = Endpoint{
			ID:        endpointID,
			Name:      "endpoint-" + endpointID[:8],
			TargetURL: "https://example.com",
			Provider:  "unknown",
			CreatedAt: time.Now(),
			IsActive:  true,
		}
	}

	eventType := r.Header.Get("X-Webhook-Event")
	if eventType == "" {
		eventType = "webhook.event"
	}

	event := Event{
		ID:         time.Now().Format("20060102150405"),
		EndpointID: endpointID,
		EventType:  eventType,
		Payload:    map[string]any{"headers": r.Header},
		Status:     "received",
		CreatedAt:  time.Now(),
	}
	events = append(events, event)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{"status": "received", "event_id": event.ID})
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]any{
		"events": events,
		"count":  len(events),
	})
}

func endpointsHandler(w http.ResponseWriter, r *http.Request) {
	eps := make([]Endpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		eps = append(eps, ep)
	}
	json.NewEncoder(w).Encode(map[string]any{
		"endpoints": eps,
		"count":     len(eps),
	})
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/health", healthHandler).Methods("GET")
	r.HandleFunc("/webhook/{endpoint_id}", webhookHandler).Methods("POST")
	r.HandleFunc("/events", eventsHandler).Methods("GET")
	r.HandleFunc("/endpoints", endpointsHandler).Methods("GET")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("HookKeeper API starting on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, handlers.CORS()(r)))
}
