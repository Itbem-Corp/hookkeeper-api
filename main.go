package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/redis/go-redis/v9"
)

const (
	rateLimitRequests = 1000
	rateLimitWindow   = time.Hour

	endpointsCacheTTL = 30 * time.Second
	eventsCacheTTL    = 30 * time.Second
	endpointCacheTTL  = 60 * time.Second
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

type cacheStore interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
}

type rateLimiter interface {
	Check(ctx context.Context, key string, limit int64, window time.Duration) (rateLimitResult, error)
}

type cacheEntry struct {
	value     []byte
	expiresAt time.Time
}

type memoryCache struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry
}

func newMemoryCache() *memoryCache {
	return &memoryCache{entries: make(map[string]cacheEntry)}
}

func (m *memoryCache) Get(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.RLock()
	entry, ok := m.entries[key]
	m.mu.RUnlock()
	if !ok {
		return nil, false, nil
	}
	if time.Now().After(entry.expiresAt) {
		m.mu.Lock()
		delete(m.entries, key)
		m.mu.Unlock()
		return nil, false, nil
	}
	return append([]byte(nil), entry.value...), true, nil
}

func (m *memoryCache) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	m.entries[key] = cacheEntry{
		value:     append([]byte(nil), value...),
		expiresAt: time.Now().Add(ttl),
	}
	m.mu.Unlock()
	return nil
}

func (m *memoryCache) Delete(_ context.Context, keys ...string) error {
	m.mu.Lock()
	for _, key := range keys {
		delete(m.entries, key)
	}
	m.mu.Unlock()
	return nil
}

type redisCache struct {
	client *redis.Client
}

func (r *redisCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	val, err := r.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (r *redisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return r.client.Set(ctx, key, value, ttl).Err()
}

func (r *redisCache) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return r.client.Del(ctx, keys...).Err()
}

type hybridCache struct {
	primary  *redisCache
	fallback *memoryCache
}

func newHybridCache(client *redis.Client) *hybridCache {
	var primary *redisCache
	if client != nil {
		primary = &redisCache{client: client}
	}
	return &hybridCache{
		primary:  primary,
		fallback: newMemoryCache(),
	}
}

func (h *hybridCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if h.primary != nil {
		val, ok, err := h.primary.Get(ctx, key)
		if err == nil {
			return val, ok, nil
		}
		log.Printf("redis cache get failed, using memory fallback: %v", err)
	}
	return h.fallback.Get(ctx, key)
}

func (h *hybridCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if h.primary != nil {
		if err := h.primary.Set(ctx, key, value, ttl); err == nil {
			return nil
		} else {
			log.Printf("redis cache set failed, using memory fallback: %v", err)
		}
	}
	return h.fallback.Set(ctx, key, value, ttl)
}

func (h *hybridCache) Delete(ctx context.Context, keys ...string) error {
	var errs []string
	if h.primary != nil {
		if err := h.primary.Delete(ctx, keys...); err != nil {
			errs = append(errs, err.Error())
			log.Printf("redis cache delete failed, using memory fallback: %v", err)
		}
	}
	if err := h.fallback.Delete(ctx, keys...); err != nil {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, "; "))
	}
	return nil
}

type rateLimitResult struct {
	Limit     int64
	Remaining int64
	ResetAt   time.Time
	Allowed   bool
}

type memoryRateLimiter struct {
	mu      sync.Mutex
	entries map[string]*memoryRateLimitEntry
}

type memoryRateLimitEntry struct {
	count     int64
	expiresAt time.Time
}

func newMemoryRateLimiter() *memoryRateLimiter {
	return &memoryRateLimiter{entries: make(map[string]*memoryRateLimitEntry)}
}

func (m *memoryRateLimiter) Check(_ context.Context, key string, limit int64, window time.Duration) (rateLimitResult, error) {
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[key]
	if !ok || now.After(entry.expiresAt) {
		entry = &memoryRateLimitEntry{expiresAt: now.Add(window)}
		m.entries[key] = entry
	}

	entry.count++
	remaining := limit - entry.count
	if remaining < 0 {
		remaining = 0
	}

	return rateLimitResult{
		Limit:     limit,
		Remaining: remaining,
		ResetAt:   entry.expiresAt,
		Allowed:   entry.count <= limit,
	}, nil
}

type redisRateLimiter struct {
	client *redis.Client
}

func (r *redisRateLimiter) Check(ctx context.Context, key string, limit int64, window time.Duration) (rateLimitResult, error) {
	count, err := r.client.Incr(ctx, key).Result()
	if err != nil {
		return rateLimitResult{}, err
	}
	if count == 1 {
		if err := r.client.Expire(ctx, key, window).Err(); err != nil {
			return rateLimitResult{}, err
		}
	}

	ttl, err := r.client.TTL(ctx, key).Result()
	if err != nil {
		return rateLimitResult{}, err
	}
	if ttl <= 0 {
		if err := r.client.Expire(ctx, key, window).Err(); err != nil {
			return rateLimitResult{}, err
		}
		ttl = window
	}

	remaining := limit - count
	if remaining < 0 {
		remaining = 0
	}

	return rateLimitResult{
		Limit:     limit,
		Remaining: remaining,
		ResetAt:   time.Now().Add(ttl),
		Allowed:   count <= limit,
	}, nil
}

type hybridRateLimiter struct {
	primary  *redisRateLimiter
	fallback *memoryRateLimiter
}

func newHybridRateLimiter(client *redis.Client) *hybridRateLimiter {
	var primary *redisRateLimiter
	if client != nil {
		primary = &redisRateLimiter{client: client}
	}
	return &hybridRateLimiter{
		primary:  primary,
		fallback: newMemoryRateLimiter(),
	}
}

func (h *hybridRateLimiter) Check(ctx context.Context, key string, limit int64, window time.Duration) (rateLimitResult, error) {
	if h.primary != nil {
		result, err := h.primary.Check(ctx, key, limit, window)
		if err == nil {
			return result, nil
		}
		log.Printf("redis rate limit check failed, using memory fallback: %v", err)
	}
	return h.fallback.Check(ctx, key, limit, window)
}

type app struct {
	mu          sync.RWMutex
	endpoints   map[string]Endpoint
	events      []Event
	cache       cacheStore
	rateLimiter rateLimiter
	redisClient *redis.Client
	redisURL    string
}

func newApp(redisURL string) *app {
	client := initRedis(redisURL)
	return &app{
		endpoints:   make(map[string]Endpoint),
		events:      make([]Event, 0),
		cache:       newHybridCache(client),
		rateLimiter: newHybridRateLimiter(client),
		redisClient: client,
		redisURL:    redisURL,
	}
}

func initRedis(redisURL string) *redis.Client {
	if strings.TrimSpace(redisURL) == "" {
		return nil
	}

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Printf("invalid REDIS_URL, using in-memory fallback: %v", err)
		return nil
	}

	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("redis unavailable, using in-memory fallback: %v", err)
		_ = client.Close()
		return nil
	}

	log.Printf("redis connected")
	return client
}

func (a *app) healthHandler(w http.ResponseWriter, r *http.Request) {
	response := map[string]any{
		"status":  "ok",
		"service": "hookkeeper-api",
		"redis":   a.redisStatus(r.Context()),
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *app) webhookHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	endpointID := vars["endpoint_id"]

	a.mu.Lock()
	if _, ok := a.endpoints[endpointID]; !ok {
		nameSuffix := endpointID
		if len(nameSuffix) > 8 {
			nameSuffix = nameSuffix[:8]
		}
		a.endpoints[endpointID] = Endpoint{
			ID:        endpointID,
			Name:      "endpoint-" + nameSuffix,
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
	a.events = append(a.events, event)
	a.mu.Unlock()

	a.invalidateCache(r.Context(),
		"GET:/events",
		"GET:/endpoints",
		fmt.Sprintf("GET:/endpoints/%s", endpointID),
	)

	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "received",
		"event_id": event.ID,
	})
}

func (a *app) eventsHandler(w http.ResponseWriter, r *http.Request) {
	a.respondWithCache(w, r, "GET:/events", eventsCacheTTL, func() (any, error) {
		a.mu.RLock()
		defer a.mu.RUnlock()

		items := make([]Event, len(a.events))
		copy(items, a.events)
		return map[string]any{
			"events": items,
			"count":  len(items),
		}, nil
	})
}

func (a *app) endpointsHandler(w http.ResponseWriter, r *http.Request) {
	a.respondWithCache(w, r, "GET:/endpoints", endpointsCacheTTL, func() (any, error) {
		a.mu.RLock()
		defer a.mu.RUnlock()

		eps := make([]Endpoint, 0, len(a.endpoints))
		for _, ep := range a.endpoints {
			eps = append(eps, ep)
		}

		return map[string]any{
			"endpoints": eps,
			"count":     len(eps),
		}, nil
	})
}

func (a *app) endpointHandler(w http.ResponseWriter, r *http.Request) {
	endpointID := mux.Vars(r)["endpoint_id"]
	cacheKey := fmt.Sprintf("GET:/endpoints/%s", endpointID)

	a.respondWithCache(w, r, cacheKey, endpointCacheTTL, func() (any, error) {
		a.mu.RLock()
		defer a.mu.RUnlock()

		endpoint, ok := a.endpoints[endpointID]
		if !ok {
			return nil, errNotFound("endpoint not found")
		}
		return endpoint, nil
	})
}

type notFoundError struct {
	message string
}

func (e notFoundError) Error() string {
	return e.message
}

func errNotFound(message string) error {
	return notFoundError{message: message}
}

func (a *app) respondWithCache(w http.ResponseWriter, r *http.Request, cacheKey string, ttl time.Duration, build func() (any, error)) {
	ctx := r.Context()
	if cached, ok, err := a.cache.Get(ctx, cacheKey); err == nil && ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		return
	} else if err != nil {
		log.Printf("cache read failed for %s: %v", cacheKey, err)
	}

	payload, err := build()
	if err != nil {
		if _, ok := err.(notFoundError); ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	body, err := json.Marshal(payload)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	if err := a.cache.Set(ctx, cacheKey, body, ttl); err != nil {
		log.Printf("cache write failed for %s: %v", cacheKey, err)
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func (a *app) invalidateCache(ctx context.Context, keys ...string) {
	if err := a.cache.Delete(ctx, keys...); err != nil {
		log.Printf("cache invalidation failed: %v", err)
	}
}

func (a *app) rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientID := clientIdentity(r)
		key := fmt.Sprintf("rate_limit:%s:%s", r.URL.Path, clientID)
		result, err := a.rateLimiter.Check(r.Context(), key, rateLimitRequests, rateLimitWindow)
		if err != nil {
			log.Printf("rate limiter failed open for %s: %v", key, err)
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", result.Limit))
		w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))
		w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", result.ResetAt.Unix()))

		if !result.Allowed {
			writeJSON(w, http.StatusTooManyRequests, map[string]string{
				"error": "rate limit exceeded",
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (a *app) redisStatus(ctx context.Context) map[string]any {
	configured := strings.TrimSpace(a.redisURL) != ""
	if a.redisClient == nil {
		return map[string]any{
			"configured": configured,
			"available":  false,
			"mode":       "memory_fallback",
		}
	}

	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := a.redisClient.Ping(checkCtx).Err(); err != nil {
		return map[string]any{
			"configured": configured,
			"available":  false,
			"mode":       "memory_fallback",
			"error":      err.Error(),
		}
	}

	return map[string]any{
		"configured": configured,
		"available":  true,
		"mode":       "redis",
	}
}

func clientIdentity(r *http.Request) string {
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		parts := strings.Split(forwarded, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}
	if realIP := strings.TrimSpace(r.Header.Get("X-Real-IP")); realIP != "" {
		return realIP
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func main() {
	redisURL := os.Getenv("REDIS_URL")
	app := newApp(redisURL)

	r := mux.NewRouter()
	r.Use(app.rateLimitMiddleware)

	r.HandleFunc("/health", app.healthHandler).Methods(http.MethodGet)
	r.HandleFunc("/webhook/{endpoint_id}", app.webhookHandler).Methods(http.MethodPost)
	r.HandleFunc("/events", app.eventsHandler).Methods(http.MethodGet)
	r.HandleFunc("/endpoints", app.endpointsHandler).Methods(http.MethodGet)
	r.HandleFunc("/endpoints/{endpoint_id}", app.endpointHandler).Methods(http.MethodGet)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("HookKeeper API starting on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, handlers.CORS()(r)))
}
