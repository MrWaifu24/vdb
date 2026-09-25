package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Item struct {
	Value      any   `json:"Value"`
	Expiration int64 `json:"Expiration,omitempty"`
	Priority   int   `json:"Priority,omitempty"`
}

type Transaction struct {
	Action string `json:"Action"`
	Key    string `json:"Key"`
	Item   Item   `json:"Item,omitempty"`
}

type Store struct {
	mu           sync.RWMutex
	logMu        sync.Mutex
	items        map[string]Item
	logFile      *os.File
	logPath      string
	maxKeys      int
	startTime    time.Time
	readsCount   atomic.Uint64
	writesCount  atomic.Uint64
	deletesCount atomic.Uint64
	expiredCount atomic.Uint64
}

func NewStore(logPath string, maxKeys int) (*Store, error) {
	s := &Store{
		items:     make(map[string]Item),
		logPath:   logPath,
		maxKeys:   maxKeys,
		startTime: time.Now(),
	}

	if err := s.loadFromLog(); err != nil {
		return nil, fmt.Errorf("failed to load transaction log: %w", err)
	}

	var err error
	s.logFile, err = os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open transaction log: %w", err)
	}

	return s, nil
}

func (s *Store) loadFromLog() error {
	file, err := os.Open(s.logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	now := time.Now().UnixNano()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var tx Transaction
		if err := json.Unmarshal(line, &tx); err != nil {
			log.Printf("error unmarshalling transaction: %v", err)
			continue
		}

		switch tx.Action {
		case "set":
			if tx.Item.Expiration > 0 && now > tx.Item.Expiration {
				delete(s.items, tx.Key)
				continue
			}
			s.items[tx.Key] = tx.Item
		case "delete":
			delete(s.items, tx.Key)
		}
	}
	return scanner.Err()
}

func (s *Store) writeTransaction(tx Transaction) error {
	s.logMu.Lock()
	defer s.logMu.Unlock()

	if s.logFile == nil {
		return nil
	}

	data, err := json.Marshal(tx)
	if err != nil {
		return err
	}

	if _, err := s.logFile.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

func (s *Store) findEvictionCandidateLocked() string {
	now := time.Now().UnixNano()
	var evictKey string
	minPriority := int(^uint(0) >> 1)
	var soonestExp int64

	for k, item := range s.items {
		if item.Expiration > 0 && now > item.Expiration {
			return k
		}
		if item.Priority < minPriority {
			minPriority = item.Priority
			soonestExp = item.Expiration
			evictKey = k
		} else if item.Priority == minPriority {
			if item.Expiration > 0 && (soonestExp == 0 || item.Expiration < soonestExp) {
				soonestExp = item.Expiration
				evictKey = k
			}
		}
	}
	return evictKey
}

func (s *Store) Get(key string) (Item, bool) {
	now := time.Now().UnixNano()

	s.mu.RLock()
	item, found := s.items[key]
	s.mu.RUnlock()

	if !found {
		return Item{}, false
	}

	if item.Expiration > 0 && now > item.Expiration {
		s.mu.Lock()
		if cur, ok := s.items[key]; ok && cur.Expiration > 0 && now > cur.Expiration {
			delete(s.items, key)
			s.expiredCount.Add(1)
			_ = s.writeTransaction(Transaction{Action: "delete", Key: key})
		}
		s.mu.Unlock()
		return Item{}, false
	}

	s.readsCount.Add(1)
	return item, true
}

func (s *Store) Set(key string, val any, ttlSeconds int64, priority int) error {
	var expiration int64
	if ttlSeconds > 0 {
		expiration = time.Now().Add(time.Duration(ttlSeconds) * time.Second).UnixNano()
	}

	item := Item{
		Value:      val,
		Expiration: expiration,
		Priority:   priority,
	}

	var evictedKey string
	s.mu.Lock()
	if s.maxKeys > 0 && len(s.items) >= s.maxKeys {
		if _, exists := s.items[key]; !exists {
			evictedKey = s.findEvictionCandidateLocked()
			if evictedKey != "" {
				delete(s.items, evictedKey)
			}
		}
	}
	s.items[key] = item
	s.mu.Unlock()

	s.writesCount.Add(1)
	if evictedKey != "" {
		s.deletesCount.Add(1)
		_ = s.writeTransaction(Transaction{Action: "delete", Key: evictedKey})
	}
	return s.writeTransaction(Transaction{Action: "set", Key: key, Item: item})
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	_, exists := s.items[key]
	if exists {
		delete(s.items, key)
	}
	s.mu.Unlock()

	if exists {
		s.deletesCount.Add(1)
		_ = s.writeTransaction(Transaction{Action: "delete", Key: key})
	}
	return exists
}

func (s *Store) Keys(prefix string, minPriority int) []string {
	now := time.Now().UnixNano()
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.items))
	for k, item := range s.items {
		if item.Expiration > 0 && now > item.Expiration {
			continue
		}
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		if item.Priority < minPriority {
			continue
		}
		keys = append(keys, k)
	}
	return keys
}

func (s *Store) Stats() map[string]any {
	s.mu.RLock()
	keyCount := len(s.items)
	s.mu.RUnlock()

	return map[string]any{
		"keys":            keyCount,
		"max_keys":        s.maxKeys,
		"uptime_seconds":  int(time.Since(s.startTime).Seconds()),
		"total_reads":     s.readsCount.Load(),
		"total_writes":    s.writesCount.Load(),
		"total_deletes":   s.deletesCount.Load(),
		"expired_cleaned": s.expiredCount.Load(),
	}
}

func (s *Store) Compact() (int, error) {
	s.mu.RLock()
	now := time.Now().UnixNano()
	snapshot := make([]Transaction, 0, len(s.items))
	for k, v := range s.items {
		if v.Expiration == 0 || now <= v.Expiration {
			snapshot = append(snapshot, Transaction{Action: "set", Key: k, Item: v})
		}
	}
	s.mu.RUnlock()

	s.logMu.Lock()
	defer s.logMu.Unlock()

	tmpPath := s.logPath + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}

	writer := bufio.NewWriter(tmpFile)
	for _, tx := range snapshot {
		data, err := json.Marshal(tx)
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return 0, err
		}
		if _, err := writer.Write(append(data, '\n')); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return 0, err
		}
	}
	if err := writer.Flush(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return 0, err
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return 0, err
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return 0, err
	}

	if s.logFile != nil {
		_ = s.logFile.Close()
	}

	if err := os.Rename(tmpPath, s.logPath); err != nil {
		return 0, err
	}

	s.logFile, err = os.OpenFile(s.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}

	return len(snapshot), nil
}

func (s *Store) cleanupExpired() {
	var expired []string
	now := time.Now().UnixNano()

	s.mu.Lock()
	for key, item := range s.items {
		if item.Expiration > 0 && now > item.Expiration {
			delete(s.items, key)
			expired = append(expired, key)
		}
	}
	s.mu.Unlock()

	if len(expired) == 0 {
		return
	}

	s.expiredCount.Add(uint64(len(expired)))
	for _, key := range expired {
		_ = s.writeTransaction(Transaction{Action: "delete", Key: key})
	}
}

func (s *Store) StartJanitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cleanupExpired()
		}
	}
}

func (s *Store) Close() error {
	s.logMu.Lock()
	defer s.logMu.Unlock()

	if s.logFile != nil {
		_ = s.logFile.Sync()
		err := s.logFile.Close()
		s.logFile = nil
		return err
	}
	return nil
}

// HTTP Layer

type Server struct {
	store *Store
}

func NewServer(store *Store) *Server {
	return &Server{store: store}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func (srv *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// REST API (Go 1.22+ patterns)
	mux.HandleFunc("GET /keys/{key}", srv.handleGetKey)
	mux.HandleFunc("GET /keys", srv.handleListKeys)
	mux.HandleFunc("POST /keys", srv.handleSetKey)
	mux.HandleFunc("PUT /keys/{key}", srv.handleSetKey)
	mux.HandleFunc("DELETE /keys/{key}", srv.handleDeleteKey)
	mux.HandleFunc("GET /stats", srv.handleStats)
	mux.HandleFunc("POST /compact", srv.handleCompact)

	// Legacy backward-compatible endpoints
	mux.HandleFunc("GET /get", srv.handleLegacyGet)
	mux.HandleFunc("POST /set", srv.handleLegacySet)
	mux.HandleFunc("DELETE /delete", srv.handleLegacyDelete)

	return mux
}

func (srv *Server) handleGetKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		key = r.URL.Query().Get("key")
	}
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}

	item, found := srv.store.Get(key)
	if !found {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}

	if r.URL.Query().Get("raw") == "true" {
		writeJSON(w, http.StatusOK, item.Value)
		return
	}

	var ttlRemaining int64
	if item.Expiration > 0 {
		ttlRemaining = (item.Expiration - time.Now().UnixNano()) / int64(time.Second)
		if ttlRemaining < 0 {
			ttlRemaining = 0
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"key":        key,
		"value":      item.Value,
		"priority":   item.Priority,
		"ttl":        ttlRemaining,
		"expiration": item.Expiration,
	})
}

func (srv *Server) handleLegacyGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	item, found := srv.store.Get(key)
	if !found {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(item.Value)
}

type setPayload struct {
	Key      string `json:"key"`
	Value    any    `json:"value"`
	TTL      int64  `json:"ttl"`
	Priority int    `json:"priority"`
}

func (srv *Server) handleSetKey(w http.ResponseWriter, r *http.Request) {
	var payload setPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json payload: %v", err))
		return
	}

	if pathKey := r.PathValue("key"); pathKey != "" {
		payload.Key = pathKey
	}

	if payload.Key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}

	if err := srv.store.Set(payload.Key, payload.Value, payload.TTL, payload.Priority); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"key":    payload.Key,
	})
}

func (srv *Server) handleLegacySet(w http.ResponseWriter, r *http.Request) {
	var payload setPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if payload.Key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	if err := srv.store.Set(payload.Key, payload.Value, payload.TTL, payload.Priority); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (srv *Server) handleDeleteKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		key = r.URL.Query().Get("key")
	}
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}

	if !srv.store.Delete(key) {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "deleted",
		"key":    key,
	})
}

func (srv *Server) handleLegacyDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	srv.store.Delete(key)
	w.WriteHeader(http.StatusNoContent)
}

func (srv *Server) handleListKeys(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	var minPriority int
	if p := r.URL.Query().Get("min_priority"); p != "" {
		fmt.Sscanf(p, "%d", &minPriority)
	}

	keys := srv.store.Keys(prefix, minPriority)
	writeJSON(w, http.StatusOK, map[string]any{
		"keys":  keys,
		"count": len(keys),
	})
}

func (srv *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := srv.store.Stats()
	writeJSON(w, http.StatusOK, stats)
}

func (srv *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	count, err := srv.store.Compact()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "compacted",
		"keys_retained": count,
	})
}

func main() {
	port := flag.Int("port", 6969, "HTTP server port")
	logPath := flag.String("log", "transaction.log", "Transaction log file path")
	maxKeys := flag.Int("max-keys", 0, "Max in-memory keys (0 for unlimited; lowest priority evicted when full)")
	cleanupInterval := flag.Duration("cleanup", 1*time.Second, "Janitor cleanup interval")
	flag.Parse()

	log.Println("Loading from previous state...")
	store, err := NewStore(*logPath, *maxKeys)
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go store.StartJanitor(ctx, *cleanupInterval)

	srv := NewServer(store)
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: srv.Routes(),
	}

	go func() {
		log.Printf("Server starting on port %d", *port)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down server gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
	if err := store.Close(); err != nil {
		log.Printf("Store close error: %v", err)
	}
	log.Println("Server stopped")
}
