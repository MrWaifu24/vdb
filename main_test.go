package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T, maxKeys int) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test_transaction.log")
	store, err := NewStore(logPath, maxKeys)
	if err != nil {
		t.Fatalf("failed to create test store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store, logPath
}

func TestStore_SetGetDelete(t *testing.T) {
	store, _ := newTestStore(t, 0)

	// Set key
	err := store.Set("user:1", "alice", 0, 1)
	if err != nil {
		t.Fatalf("unexpected error on Set: %v", err)
	}

	// Get key
	item, ok := store.Get("user:1")
	if !ok {
		t.Fatalf("expected key user:1 to be found")
	}
	if item.Value != "alice" {
		t.Fatalf("expected value 'alice', got %v", item.Value)
	}
	if item.Priority != 1 {
		t.Fatalf("expected priority 1, got %d", item.Priority)
	}

	// Delete key
	deleted := store.Delete("user:1")
	if !deleted {
		t.Fatalf("expected user:1 to be deleted")
	}

	// Get deleted key
	_, ok = store.Get("user:1")
	if ok {
		t.Fatalf("expected user:1 to not be found after delete")
	}
}

func TestStore_LazyExpiration(t *testing.T) {
	store, _ := newTestStore(t, 0)

	// Set with 1 nanosecond expiration (expired in the past)
	store.mu.Lock()
	store.items["expired_key"] = Item{
		Value:      "old_data",
		Expiration: time.Now().Add(-1 * time.Second).UnixNano(),
		Priority:   0,
	}
	store.mu.Unlock()

	// Get should trigger lazy expiration and return false
	_, found := store.Get("expired_key")
	if found {
		t.Fatalf("expected expired key to not be found")
	}

	// Verify key was removed from store
	store.mu.RLock()
	_, exists := store.items["expired_key"]
	store.mu.RUnlock()
	if exists {
		t.Fatalf("expected expired key to be deleted from in-memory map")
	}
}

func TestStore_JanitorCleanup(t *testing.T) {
	store, _ := newTestStore(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go store.StartJanitor(ctx, 10*time.Millisecond)

	// Insert item with very short TTL
	err := store.Set("temp_key", "ephemeral", 1, 0)
	if err != nil {
		t.Fatalf("unexpected error on Set: %v", err)
	}

	// Manually set expiration to past
	store.mu.Lock()
	it := store.items["temp_key"]
	it.Expiration = time.Now().Add(-100 * time.Millisecond).UnixNano()
	store.items["temp_key"] = it
	store.mu.Unlock()

	// Wait for janitor tick
	time.Sleep(50 * time.Millisecond)

	store.mu.RLock()
	_, exists := store.items["temp_key"]
	store.mu.RUnlock()
	if exists {
		t.Fatalf("expected janitor to remove expired item")
	}
}

func TestStore_PriorityEviction(t *testing.T) {
	store, _ := newTestStore(t, 2) // max 2 keys

	// Set two keys with different priorities
	_ = store.Set("k1", "v1", 0, 5) // high priority
	_ = store.Set("k2", "v2", 0, 1) // low priority

	// Add 3rd key, should evict k2 (lowest priority)
	_ = store.Set("k3", "v3", 0, 3)

	if _, found := store.Get("k2"); found {
		t.Fatalf("expected k2 (lowest priority) to be evicted")
	}
	if _, found := store.Get("k1"); !found {
		t.Fatalf("expected k1 to be retained")
	}
	if _, found := store.Get("k3"); !found {
		t.Fatalf("expected k3 to be retained")
	}
}

func TestStore_PersistenceReplay(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "persist.log")

	s1, err := NewStore(logPath, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = s1.Set("persisted", "saved_value", 0, 2)
	_ = s1.Set("to_delete", "temporary", 0, 0)
	_ = s1.Delete("to_delete")
	// Set an already-expired key
	s1.mu.Lock()
	s1.items["already_expired"] = Item{
		Value:      "dead",
		Expiration: time.Now().Add(-10 * time.Second).UnixNano(),
	}
	_ = s1.writeTransaction(Transaction{
		Action: "set",
		Key:    "already_expired",
		Item:   s1.items["already_expired"],
	})
	s1.mu.Unlock()
	_ = s1.Close()

	// Reopen with new store instance
	s2, err := NewStore(logPath, 0)
	if err != nil {
		t.Fatalf("unexpected error on reopen: %v", err)
	}
	defer s2.Close()

	item, found := s2.Get("persisted")
	if !found || item.Value != "saved_value" {
		t.Fatalf("expected persisted key to be replayed, got %v", item.Value)
	}

	if _, found := s2.Get("to_delete"); found {
		t.Fatalf("expected to_delete to not be present after replay")
	}

	if _, found := s2.Get("already_expired"); found {
		t.Fatalf("expected already_expired to be skipped on replay")
	}
}

func TestStore_Compaction(t *testing.T) {
	store, _ := newTestStore(t, 0)

	// Set key multiple times
	for i := 0; i < 20; i++ {
		_ = store.Set("counter", i, 0, 0)
	}

	retained, err := store.Compact()
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}
	if retained != 1 {
		t.Fatalf("expected 1 key retained, got %d", retained)
	}

	item, found := store.Get("counter")
	if !found || item.Value != float64(19) && item.Value != 19 {
		t.Fatalf("expected counter=19 after compaction, got %v", item.Value)
	}
}

func TestHTTP_ModernAndLegacyEndpoints(t *testing.T) {
	store, _ := newTestStore(t, 0)
	server := NewServer(store)
	router := server.Routes()

	// 1. Modern POST /keys
	body, _ := json.Marshal(map[string]any{
		"key":      "hero",
		"value":    "batman",
		"ttl":      300,
		"priority": 10,
	})
	req := httptest.NewRequest("POST", "/keys", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// 2. Modern GET /keys/{key}
	req = httptest.NewRequest("GET", "/keys/hero", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var getResp struct {
		Key      string `json:"key"`
		Value    any    `json:"value"`
		Priority int    `json:"priority"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &getResp)
	if getResp.Value != "batman" || getResp.Priority != 10 {
		t.Fatalf("unexpected GET response: %+v", getResp)
	}

	// 3. GET /keys/{key}?raw=true
	req = httptest.NewRequest("GET", "/keys/hero?raw=true", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var rawVal string
	_ = json.Unmarshal(rec.Body.Bytes(), &rawVal)
	if rawVal != "batman" {
		t.Fatalf("expected 'batman', got %s", rawVal)
	}

	// 4. GET /keys (list)
	req = httptest.NewRequest("GET", "/keys", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var listResp struct {
		Keys  []string `json:"keys"`
		Count int      `json:"count"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &listResp)
	if listResp.Count != 1 || listResp.Keys[0] != "hero" {
		t.Fatalf("unexpected list response: %+v", listResp)
	}

	// 5. GET /stats
	req = httptest.NewRequest("GET", "/stats", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// 6. POST /compact
	req = httptest.NewRequest("POST", "/compact", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// 7. Legacy GET /get?key=hero
	req = httptest.NewRequest("GET", "/get?key=hero", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// 8. Modern DELETE /keys/{key}
	req = httptest.NewRequest("DELETE", "/keys/hero", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// 9. Legacy POST /set
	body, _ = json.Marshal(map[string]any{
		"key":   "legacy_key",
		"value": "legacy_val",
	})
	req = httptest.NewRequest("POST", "/set", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// 10. Legacy DELETE /delete?key=legacy_key
	req = httptest.NewRequest("DELETE", "/delete?key=legacy_key", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
}

func TestStore_Concurrency(t *testing.T) {
	store, _ := newTestStore(t, 0)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(2)
		key := fmt.Sprintf("concur_%d", i)
		go func(k string) {
			defer wg.Done()
			_ = store.Set(k, "val", 0, 0)
		}(key)

		go func(k string) {
			defer wg.Done()
			_, _ = store.Get(k)
		}(key)
	}

	wg.Wait()
}

func newBenchmarkStore(b *testing.B, maxKeys int) *Store {
	b.Helper()
	dir := b.TempDir()
	logPath := filepath.Join(dir, "bench_transaction.log")
	store, err := NewStore(logPath, maxKeys)
	if err != nil {
		b.Fatalf("failed to create bench store: %v", err)
	}
	b.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func BenchmarkStore_Set(b *testing.B) {
	store := newBenchmarkStore(b, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Set("bench_key", "bench_value", 300, 1)
	}
}

func BenchmarkStore_Get(b *testing.B) {
	store := newBenchmarkStore(b, 0)
	_ = store.Set("bench_key", "bench_value", 300, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Get("bench_key")
	}
}

func BenchmarkStore_ParallelGet(b *testing.B) {
	store := newBenchmarkStore(b, 0)
	_ = store.Set("bench_key", "bench_value", 300, 1)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = store.Get("bench_key")
		}
	})
}

func BenchmarkStore_ParallelSet(b *testing.B) {
	store := newBenchmarkStore(b, 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("pk_%d", i%1000)
			_ = store.Set(key, "parallel_val", 300, 1)
			i++
		}
	})
}

func BenchmarkHTTP_Get(b *testing.B) {
	store := newBenchmarkStore(b, 0)
	_ = store.Set("bench_key", "bench_value", 300, 1)
	server := NewServer(store)
	router := server.Routes()
	req := httptest.NewRequest("GET", "/keys/bench_key", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
	}
}

func BenchmarkHTTP_Set(b *testing.B) {
	store := newBenchmarkStore(b, 0)
	server := NewServer(store)
	router := server.Routes()
	payload := []byte(`{"key":"bench_key","value":"bench_value","ttl":300,"priority":1}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/keys", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
	}
}
