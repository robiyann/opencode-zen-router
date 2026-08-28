package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Embed Dashboard HTML directly into the binary!
//
//go:embed web/dashboard.html
var dashboardHTML []byte

const (
	DefaultPort    = "8080"
	DefaultDBFile  = "zyrouter.db"
	CooldownPeriod = 45 * time.Second
	MaxRetries     = 3
	VercelAPI      = "https://api.vercel.com"
)

type ProxyNode struct {
	ID         int64
	URL        *url.URL
	RawURL     string
	CooldownTo time.Time
	InFlight   atomic.Int32
	mu         sync.RWMutex
}

func (p *ProxyNode) IsAvailable() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return time.Now().After(p.CooldownTo)
}

func (p *ProxyNode) CooldownRemaining() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	remain := time.Until(p.CooldownTo)
	if remain > 0 {
		return int(remain.Seconds())
	}
	return 0
}

func (p *ProxyNode) SetCooldown(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.CooldownTo = time.Now().Add(d)
}

func (p *ProxyNode) SetCooldownForStatus(statusCode int) time.Duration {
	var d time.Duration
	switch statusCode {
	case 429:
		d = 8 * time.Second // Short burst recovery for rate limits
	case 504:
		d = 15 * time.Second // Gateway timeout
	default:
		d = 30 * time.Second // Server error
	}
	p.SetCooldown(d)
	return d
}

// EventHub manages live SSE connections to the Dashboard
type EventHub struct {
	clients map[chan []byte]bool
	mu      sync.Mutex
}

func NewEventHub() *EventHub {
	return &EventHub{
		clients: make(map[chan []byte]bool),
	}
}

func (h *EventHub) Subscribe() chan []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan []byte, 32)
	h.clients[ch] = true
	return ch
}

func (h *EventHub) Unsubscribe(ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, ch)
	close(ch)
}

func (h *EventHub) Broadcast(eventType string, data interface{}) {
	payload, err := json.Marshal(map[string]interface{}{
		"event": eventType,
		"data":  data,
		"time":  time.Now().Format("15:04:05"),
	})
	if err != nil {
		return
	}

	msg := []byte(fmt.Sprintf("data: %s\n\n", payload))

	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
		}
	}
}

// In-memory Proxy Pool
type ProxyPool struct {
	nodes        []*ProxyNode
	counter      atomic.Uint64
	stickyMap    sync.Map // key: sessionID -> value: proxy URL
	inFlight     atomic.Int64
	totalRouted  atomic.Uint64
	totalRetries atomic.Uint64
	strategy     string
	mu           sync.RWMutex
}

func NewProxyPool() *ProxyPool {
	return &ProxyPool{
		nodes:    make([]*ProxyNode, 0),
		strategy: "round-robin",
	}
}

func (pool *ProxyPool) SetStrategy(s string) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	pool.strategy = s
}

func (pool *ProxyPool) GetStrategy() string {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return pool.strategy
}

func (pool *ProxyPool) SyncFromDB(db *Database) error {
	urls, err := db.GetActiveProxies()
	if err != nil {
		return err
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.nodes = make([]*ProxyNode, 0)
	for _, u := range urls {
		parsed, err := url.Parse(u)
		if err != nil {
			continue
		}
		pool.nodes = append(pool.nodes, &ProxyNode{
			URL:    parsed,
			RawURL: strings.TrimRight(u, "/"),
		})
	}
	return nil
}

func (pool *ProxyPool) Pick(sessionID string) (*ProxyNode, int) {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	n := len(pool.nodes)
	if n == 0 {
		return nil, -1
	}

	strategy := pool.strategy

	if strategy == "sticky" && sessionID != "" {
		if val, ok := pool.stickyMap.Load(sessionID); ok {
			stickyURL := val.(string)
			for i, node := range pool.nodes {
				if node.RawURL == stickyURL && node.IsAvailable() {
					return node, i
				}
			}
		}
	}

	// 1. Filter currently available nodes
	var available []*ProxyNode
	var availableIndices []int
	for i, node := range pool.nodes {
		if node.IsAvailable() {
			available = append(available, node)
			availableIndices = append(availableIndices, i)
		}
	}

	// 2. Emergency fallback if all nodes are in cooldown: pick the one recovering earliest
	if len(available) == 0 {
		var soonest *ProxyNode
		var soonestIdx int
		earliest := time.Now().Add(24 * time.Hour)
		for i, node := range pool.nodes {
			node.mu.RLock()
			cd := node.CooldownTo
			node.mu.RUnlock()
			if cd.Before(earliest) {
				earliest = cd
				soonest = node
				soonestIdx = i
			}
		}
		if soonest != nil {
			return soonest, soonestIdx
		}
		return pool.nodes[0], 0
	}

	// 3. Least-Connection Filter: Prefer idle nodes (InFlight == 0)
	var idle []*ProxyNode
	var idleIndices []int
	for k, node := range available {
		if node.InFlight.Load() == 0 {
			idle = append(idle, node)
			idleIndices = append(idleIndices, availableIndices[k])
		}
	}

	targetPool := available
	targetIndices := availableIndices
	if len(idle) > 0 {
		targetPool = idle
		targetIndices = idleIndices
	}

	if strategy == "random" {
		bn, _ := rand.Int(rand.Reader, big.NewInt(int64(len(targetPool))))
		idx := int(bn.Int64())
		chosen := targetPool[idx]
		if sessionID != "" {
			pool.stickyMap.Store(sessionID, chosen.RawURL)
		}
		return chosen, targetIndices[idx]
	}

	// Filtered Round-Robin: Distributes evenly across all available/idle nodes
	idx := int(pool.counter.Add(1) % uint64(len(targetPool)))
	chosen := targetPool[idx]
	if sessionID != "" {
		pool.stickyMap.Store(sessionID, chosen.RawURL)
	}
	return chosen, targetIndices[idx]
}

func (pool *ProxyPool) TotalNodes() int {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return len(pool.nodes)
}

func genRandomHex(bytesCount int) string {
	b := make([]byte, bytesCount)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// In-memory Brute Force Protection for Admin Login
type LoginLimiter struct {
	attempts  map[string]int
	lockUntil map[string]time.Time
	mu        sync.Mutex
}

func NewLoginLimiter() *LoginLimiter {
	return &LoginLimiter{
		attempts:  make(map[string]int),
		lockUntil: make(map[string]time.Time),
	}
}

func (l *LoginLimiter) IsLocked(ip string) (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	until, ok := l.lockUntil[ip]
	if ok && time.Now().Before(until) {
		return true, int(time.Until(until).Seconds())
	}
	return false, 0
}

func (l *LoginLimiter) RecordFail(ip string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.attempts[ip]++
	if l.attempts[ip] >= 5 {
		l.lockUntil[ip] = time.Now().Add(5 * time.Minute)
		l.attempts[ip] = 0
		return 300
	}
	return 0
}

func (l *LoginLimiter) RecordSuccess(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.attempts, ip)
	delete(l.lockUntil, ip)
}

type CacheItem struct {
	Body      []byte
	Header    http.Header
	CreatedAt time.Time
}

type PromptCache struct {
	mu    sync.RWMutex
	items map[string]CacheItem
	ttl   time.Duration
}

func NewPromptCache(ttl time.Duration) *PromptCache {
	c := &PromptCache{
		items: make(map[string]CacheItem),
		ttl:   ttl,
	}
	go c.startCleanup()
	return c
}

func (c *PromptCache) Get(key string) ([]byte, http.Header, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	item, ok := c.items[key]
	if !ok || time.Since(item.CreatedAt) > c.ttl {
		return nil, nil, false
	}
	return item.Body, item.Header, true
}

func (c *PromptCache) Set(key string, body []byte, header http.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.items) >= 2000 {
		for k := range c.items {
			delete(c.items, k)
			break
		}
	}
	c.items[key] = CacheItem{
		Body:      body,
		Header:    header.Clone(),
		CreatedAt: time.Now(),
	}
}

func (c *PromptCache) startCleanup() {
	ticker := time.NewTicker(10 * time.Minute)
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for k, v := range c.items {
			if now.Sub(v.CreatedAt) > c.ttl {
				delete(c.items, k)
			}
		}
		c.mu.Unlock()
	}
}

// ----------------------------------------------------
// Upstream Multi-Key Round-Robin Pool with Cooldown
// ----------------------------------------------------
type UpstreamKeyPool struct {
	mu        sync.RWMutex
	keys      []string
	counter   atomic.Uint64
	cooldowns map[string]time.Time
}

func NewUpstreamKeyPool() *UpstreamKeyPool {
	return &UpstreamKeyPool{
		keys:      make([]string, 0),
		cooldowns: make(map[string]time.Time),
	}
}

func (p *UpstreamKeyPool) SetKeys(keys []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys = keys
}

func (p *UpstreamKeyPool) PickKey() (string, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	n := len(p.keys)
	if n == 0 {
		return "", 0
	}

	startIdx := int(p.counter.Add(1) - 1)
	for i := 0; i < n; i++ {
		idx := (startIdx + i) % n
		k := p.keys[idx]
		if until, inCooldown := p.cooldowns[k]; !inCooldown || now.After(until) {
			delete(p.cooldowns, k)
			return k, len(p.keys)
		}
	}

	// If all keys are in cooldown, return next key as fallback
	return p.keys[startIdx%n], len(p.keys)
}

func (p *UpstreamKeyPool) MarkCooldown(key string, d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cooldowns[key] = time.Now().Add(d)
}

// Router Server
type RouterServer struct {
	db               *Database
	pool             *ProxyPool
	hub              *EventHub
	limiter          *LoginLimiter
	promptCache      *PromptCache
	httpClient       *http.Client
	providerKeyPools map[string]*UpstreamKeyPool
	keyPoolMu        sync.RWMutex
	startTime        time.Time
}

func NewRouterServer(db *Database, pool *ProxyPool, hub *EventHub) *RouterServer {
	return &RouterServer{
		db:               db,
		pool:             pool,
		hub:              hub,
		limiter:          NewLoginLimiter(),
		promptCache:      NewPromptCache(1 * time.Hour),
		providerKeyPools: make(map[string]*UpstreamKeyPool),
		httpClient: &http.Client{
			Transport: &http.Transport{
				ResponseHeaderTimeout: 65 * time.Second,
				MaxIdleConns:          500,
				MaxIdleConnsPerHost:   50,
				IdleConnTimeout:       90 * time.Second,
			},
			Timeout: 90 * time.Second,
		},
		startTime: time.Now(),
	}
}

func (s *RouterServer) GetKeyPoolForProvider(p *Provider) *UpstreamKeyPool {
	s.keyPoolMu.Lock()
	defer s.keyPoolMu.Unlock()

	pool, exists := s.providerKeyPools[p.ID]
	if !exists {
		pool = NewUpstreamKeyPool()
		s.providerKeyPools[p.ID] = pool
	}

	poolKeys := p.APIKeysPool
	if len(poolKeys) == 0 && p.APIKey != "" {
		poolKeys = []string{p.APIKey}
	}
	pool.SetKeys(poolKeys)
	return pool
}

func extractSessionID(r *http.Request, bodyBytes []byte) string {
	headers := []string{
		"x-session-id",
		"x-opencode-session",
		"session-id",
		"session_id",
		"x-conversation-id",
		"x-thread-id",
	}
	for _, h := range headers {
		val := strings.TrimSpace(r.Header.Get(h))
		if val != "" {
			return strings.ReplaceAll(strings.TrimPrefix(val, "ses_"), "-", "")
		}
	}

	if len(bodyBytes) > 0 {
		var generic map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &generic); err == nil {
			for _, k := range []string{"session_id", "conversation_id", "prompt_cache_key"} {
				if val, ok := generic[k].(string); ok && strings.TrimSpace(val) != "" {
					return strings.ReplaceAll(strings.TrimPrefix(val, "ses_"), "-", "")
				}
			}
		}
	}

	return ""
}

func enableCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-XSS-Protection", "1; mode=block")
	w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
}

// Extract Session Token from Cookie or Authorization header
func (s *RouterServer) isDashboardAuthenticated(r *http.Request) bool {
	cookie, err := r.Cookie("zen_session")
	if err == nil && s.db.ValidateSession(cookie.Value) {
		return true
	}
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if s.db.ValidateSession(token) {
			return true
		}
	}
	return false
}

// Guard middleware for Admin endpoints
func (s *RouterServer) requireAuth(w http.ResponseWriter, r *http.Request) bool {
	if !s.isDashboardAuthenticated(r) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"Unauthorized. Silakan login ke dashboard terlebih dahulu.","code":"unauthorized"}`))
		return false
	}
	return true
}

// Serve Dashboard HTML
func (s *RouterServer) HandleDashboard(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(dashboardHTML)
}

// ----------------------------------------------------
// Authentication API (Bcrypt Hashed + Rate Limited)
// ----------------------------------------------------
func (s *RouterServer) HandleAPILogin(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	remoteIP := r.RemoteAddr
	if idx := strings.LastIndex(remoteIP, ":"); idx != -1 {
		remoteIP = remoteIP[:idx]
	}

	// 1. Check Rate Limit / Lockout
	if locked, rem := s.limiter.IsLocked(remoteIP); locked {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": fmt.Sprintf("Terlalu banyak percobaan login gagal. IP terkunci selama %d detik!", rem),
			"code":  "rate_limited",
		})
		return
	}

	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"Invalid request payload"}`, http.StatusBadRequest)
		return
	}

	// 2. Verify Bcrypt Hash
	if !s.db.VerifyPassword(req.Password) {
		lockSec := s.limiter.RecordFail(remoteIP)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		msg := "Password salah!"
		if lockSec > 0 {
			msg = fmt.Sprintf("Password salah 5 kali! IP Anda dikunci selama %d detik demi keamanan.", lockSec)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"error": msg, "status": "unauthorized"})
		return
	}

	// 3. Success -> reset limiter
	s.limiter.RecordSuccess(remoteIP)

	token, err := s.db.CreateSession()
	if err != nil {
		http.Error(w, `{"error":"Failed to create session"}`, http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "zen_session",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   86400 * 30, // 30 days
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"token":  token,
	})
}

func (s *RouterServer) HandleAPILogout(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if cookie, err := r.Cookie("zen_session"); err == nil {
		s.db.DeleteSession(cookie.Value)
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "zen_session",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "logged_out"})
}

func (s *RouterServer) HandleAPIAuthMe(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	authed := s.isDashboardAuthenticated(r)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"authenticated": authed,
	})
}

func (s *RouterServer) HandleAPIChangePassword(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !s.isDashboardAuthenticated(r) {
		http.Error(w, `{"error":"Unauthorized"}`, http.StatusUnauthorized)
		return
	}

	var req struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"Invalid payload"}`, http.StatusBadRequest)
		return
	}

	if !s.db.VerifyPassword(req.CurrentPassword) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Password lama salah!"})
		return
	}

	if len(req.NewPassword) < 6 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Password baru minimal 6 karakter!"})
		return
	}

	if err := s.db.SetPassword(req.NewPassword); err != nil {
		http.Error(w, `{"error":"Failed to update password"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": "Password berhasil diubah!"})
}

// ----------------------------------------------------
// API Key Management Endpoints
// ----------------------------------------------------
func (s *RouterServer) HandleAPIKeys(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !s.requireAuth(w, r) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		keys, err := s.db.GetAPIKeys()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(keys)

	case http.MethodPost:
		var req struct {
			Name          string `json:"name"`
			AllowedModels string `json:"allowed_models"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		name := strings.TrimSpace(req.Name)
		if name == "" {
			name = "API Key"
		}
		allowed := strings.TrimSpace(req.AllowedModels)
		if allowed == "" {
			allowed = "*"
		}

		apiKey, err := s.db.CreateAPIKey(name, allowed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(apiKey)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "ID required", http.StatusBadRequest)
			return
		}
		if err := s.db.DeleteAPIKey(id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *RouterServer) HandleAPIUpdateKeyModels(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !s.requireAuth(w, r) {
		return
	}

	var req struct {
		ID            string `json:"id"`
		AllowedModels string `json:"allowed_models"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.ID == "" {
		http.Error(w, `{"error":"ID is required"}`, http.StatusBadRequest)
		return
	}

	if err := s.db.UpdateAPIKeyModels(req.ID, req.AllowedModels); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "allowed_models": req.AllowedModels})
}

func (s *RouterServer) HandleAPIToggleKey(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !s.requireAuth(w, r) {
		return
	}

	var req struct {
		ID       string `json:"id"`
		IsActive bool   `json:"is_active"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.db.ToggleAPIKey(req.ID, req.IsActive); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"is_active": req.IsActive})
}

// ----------------------------------------------------
// Multi-Provider Management API
// ----------------------------------------------------
func (s *RouterServer) HandleAPIProviders(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !s.requireAuth(w, r) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		providers, err := s.db.GetProviders()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(providers)

	case http.MethodPost:
		var p Provider
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p.Name = strings.TrimSpace(p.Name)
		p.BaseURL = strings.TrimSpace(p.BaseURL)
		p.APIKey = strings.TrimSpace(p.APIKey)
		if p.Name == "" || p.BaseURL == "" {
			http.Error(w, `{"error":"Name and Base URL are required"}`, http.StatusBadRequest)
			return
		}
		if p.ProviderType == "" {
			p.ProviderType = "openai"
		}
		p.IsActive = true
		if err := s.db.SaveProvider(&p); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "id": p.ID})

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "ID is required", http.StatusBadRequest)
			return
		}
		if id == "opencode" {
			http.Error(w, "Cannot delete default OpenCode provider", http.StatusBadRequest)
			return
		}
		if err := s.db.DeleteProvider(id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *RouterServer) HandleAPIToggleProvider(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !s.requireAuth(w, r) {
		return
	}

	var req struct {
		ID       string `json:"id"`
		IsActive bool   `json:"is_active"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.db.ToggleProvider(req.ID, req.IsActive); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"is_active": req.IsActive})
}

func (s *RouterServer) ResolveProviderAndCleanModel(modelName string) (*Provider, string) {
	activeProviders, err := s.db.GetActiveProviders()
	if err != nil || len(activeProviders) == 0 {
		return nil, modelName
	}

	cleanModel := strings.TrimSpace(modelName)
	cleanModelLower := strings.ToLower(cleanModel)

	// 1. Check if model has an explicit prefix: "<prefix>/<actual_model>"
	if strings.Contains(cleanModel, "/") {
		parts := strings.SplitN(cleanModel, "/", 2)
		prefix := strings.ToLower(strings.TrimSpace(parts[0]))
		actualModel := strings.TrimSpace(parts[1])

		for _, p := range activeProviders {
			provPrefix := strings.ToLower(strings.TrimSpace(p.ModelPrefix))
			if provPrefix == "" {
				provPrefix = strings.ToLower(p.ID)
			}
			if prefix == provPrefix || prefix == strings.ToLower(p.ID) || prefix == strings.ToLower(p.Name) {
				return &p, actualModel
			}
		}
	}

	// 2. Check explicit model matching list without prefix
	for _, p := range activeProviders {
		if p.Models != "*" && p.Models != "" && p.Models != "all" {
			parts := strings.Split(p.Models, ",")
			for _, part := range parts {
				if strings.ToLower(strings.TrimSpace(part)) == cleanModelLower {
					return &p, cleanModel
				}
			}
		}
	}

	// 3. Pattern-based routing for Genspark vs OpenCode
	isGensparkModel := strings.HasPrefix(cleanModelLower, "gpt-") ||
		strings.HasPrefix(cleanModelLower, "claude-") ||
		strings.HasPrefix(cleanModelLower, "deep-seek-") ||
		strings.HasPrefix(cleanModelLower, "deepseek-v4") ||
		strings.HasPrefix(cleanModelLower, "kimi-") ||
		strings.HasPrefix(cleanModelLower, "glm-5") ||
		strings.HasPrefix(cleanModelLower, "minimax-") ||
		strings.HasPrefix(cleanModelLower, "grok-") ||
		strings.HasPrefix(cleanModelLower, "solar-") ||
		strings.HasPrefix(cleanModelLower, "trinity-")

	if isGensparkModel {
		for _, p := range activeProviders {
			if p.ProviderType == "genspark" {
				return &p, cleanModel
			}
		}
	}

	// 4. Fallback to OpenCode fleet if model is free or default
	for _, p := range activeProviders {
		if p.ProviderType == "opencode" {
			return &p, cleanModel
		}
	}

	// 5. Any active wildcard provider
	for _, p := range activeProviders {
		if p.Models == "*" || p.Models == "all" {
			return &p, cleanModel
		}
	}

	return nil, cleanModel
}

// ----------------------------------------------------
// SSE Event Stream & Admin Endpoints
// ----------------------------------------------------
func (s *RouterServer) HandleLiveEvents(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	ch := s.hub.Subscribe()
	defer s.hub.Unsubscribe(ch)

	fmt.Fprintf(w, "data: {\"event\":\"init\",\"in_flight\":%d}\n\n", s.pool.inFlight.Load())
	flusher.Flush()

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			w.Write(msg)
			flusher.Flush()
		}
	}
}

func (s *RouterServer) HandleAPIProxies(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !s.requireAuth(w, r) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		proxies, err := s.db.GetAllProxies()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		type EnrichedProxy struct {
			DBProxy
			CooldownSeconds int `json:"cooldown_seconds"`
		}

		var enriched []EnrichedProxy
		s.pool.mu.RLock()
		nodeMap := make(map[string]*ProxyNode)
		for _, n := range s.pool.nodes {
			nodeMap[n.RawURL] = n
		}
		s.pool.mu.RUnlock()

		for _, p := range proxies {
			cd := 0
			if node, ok := nodeMap[p.URL]; ok {
				cd = node.CooldownRemaining()
			}
			enriched = append(enriched, EnrichedProxy{
				DBProxy:         p,
				CooldownSeconds: cd,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(enriched)

	case http.MethodPost:
		var req struct {
			Name string `json:"name"`
			URL  string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cleanURL := strings.TrimRight(strings.TrimSpace(req.URL), "/")
		if cleanURL == "" {
			http.Error(w, "URL is required", http.StatusBadRequest)
			return
		}
		name := strings.TrimSpace(req.Name)
		if name == "" {
			name = "Vercel Relay"
		}

		id, err := s.db.AddProxy(name, cleanURL)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.pool.SyncFromDB(s.db)
		s.hub.Broadcast("proxy_added", map[string]string{"url": cleanURL, "name": name})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"id": id, "status": "added", "url": cleanURL})

	case http.MethodDelete:
		idStr := r.URL.Query().Get("id")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, "Invalid ID", http.StatusBadRequest)
			return
		}
		if err := s.db.DeleteProxy(id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.pool.SyncFromDB(s.db)
		s.hub.Broadcast("proxy_deleted", map[string]int64{"id": id})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *RouterServer) HandleAPIToggleProxy(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !s.requireAuth(w, r) {
		return
	}

	var req struct {
		ID       int64 `json:"id"`
		IsActive bool  `json:"is_active"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.db.ToggleProxy(req.ID, req.IsActive); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.pool.SyncFromDB(s.db)
	s.hub.Broadcast("proxy_toggled", req)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"is_active": req.IsActive})
}

func (s *RouterServer) HandleAPIPingProxy(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !s.requireAuth(w, r) {
		return
	}

	var req struct {
		ID  int64  `json:"id"`
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	targetURL := strings.TrimSpace(req.URL)
	if targetURL == "" {
		http.Error(w, `{"error":"URL is required"}`, http.StatusBadRequest)
		return
	}

	t0 := time.Now()
	testPayload := []byte(`{"model":"mimo-v2.5-free","messages":[{"role":"user","content":"ping"}],"max_tokens":1}`)
	httpReq, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(testPayload))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer public")
	httpReq.Header.Set("x-opencode-client", "desktop")

	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Do(httpReq)
	latencyMs := int(time.Since(t0).Milliseconds())

	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":     502,
			"error":      err.Error(),
			"latency_ms": latencyMs,
			"ok":         false,
		})
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if req.ID > 0 {
		_ = s.db.UpdateProxyStatus(targetURL, resp.StatusCode, latencyMs, resp.StatusCode == 200)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     resp.StatusCode,
		"latency_ms": latencyMs,
		"ok":         resp.StatusCode == 200,
	})
}

func (s *RouterServer) HandleAPIStrategy(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !s.requireAuth(w, r) {
		return
	}

	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"strategy": s.pool.GetStrategy()})
		return
	}

	if r.Method == http.MethodPost {
		var req struct {
			Strategy string `json:"strategy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		clean := strings.ToLower(strings.TrimSpace(req.Strategy))
		if clean != "round-robin" && clean != "random" && clean != "sticky" {
			http.Error(w, "Invalid strategy", http.StatusBadRequest)
			return
		}
		s.pool.SetStrategy(clean)
		s.db.SetSetting("strategy", clean)
		s.hub.Broadcast("strategy_changed", map[string]string{"strategy": clean})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"strategy": clean})
		return
	}

	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

func (s *RouterServer) HandleAPILogs(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !s.requireAuth(w, r) {
		return
	}

	limit := 20
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	logs, err := s.db.GetRecentLogs(limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logs)
}

func (s *RouterServer) HandleAPIDeployVercel(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !s.requireAuth(w, r) {
		return
	}

	var req struct {
		Token  string `json:"token"`
		Prefix string `json:"prefix"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	token := strings.TrimSpace(req.Token)
	if token == "" {
		http.Error(w, `{"error":"Vercel token is required"}`, http.StatusBadRequest)
		return
	}
	prefix := strings.TrimSpace(req.Prefix)
	if prefix == "" {
		prefix = "zen-relay"
	}
	projectName := fmt.Sprintf("%s-%s", prefix, genRandomHex(4))

	relayCode := `
export const config = { runtime: "edge" };
export default async function handler(req) {
  const target = "https://opencode.ai";
  const url = target + (req.url.includes("/v1/") ? req.url.substring(req.url.indexOf("/v1/")) : "/zen/v1/chat/completions");
  const headers = new Headers(req.headers);
  headers.set("Authorization", "Bearer public");
  headers.set("x-opencode-client", "desktop");
  headers.delete("host");
  const res = await fetch(url, { method: req.method, headers, body: req.body, duplex: "half" });
  return new Response(res.body, { status: res.status, headers: res.headers });
}
`
	deployPayload := map[string]interface{}{
		"name": projectName,
		"files": []map[string]string{
			{"file": "api/chat.js", "data": relayCode},
			{"file": "package.json", "data": `{"name":"` + projectName + `","version":"1.0.0"}`},
			{"file": "vercel.json", "data": `{"rewrites":[{"source":"/(.*)","destination":"/api/chat"}]}`},
		},
		"projectSettings": map[string]interface{}{"framework": nil},
		"target":          "production",
	}

	payloadBytes, _ := json.Marshal(deployPayload)
	deployReq, _ := http.NewRequest(http.MethodPost, VercelAPI+"/v13/deployments", bytes.NewReader(payloadBytes))
	deployReq.Header.Set("Authorization", "Bearer "+token)
	deployReq.Header.Set("Content-Type", "application/json")

	deployRes, err := s.httpClient.Do(deployReq)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Vercel API error: %v"}`, err), http.StatusInternalServerError)
		return
	}
	defer deployRes.Body.Close()

	if deployRes.StatusCode >= 400 {
		b, _ := io.ReadAll(deployRes.Body)
		http.Error(w, fmt.Sprintf(`{"error":"Vercel deploy failed: %s"}`, string(b)), http.StatusBadRequest)
		return
	}

	var deployData struct {
		ID  string `json:"id"`
		URL string `json:"url"`
	}
	json.NewDecoder(deployRes.Body).Decode(&deployData)

	var finalURL string
	for i := 0; i < 20; i++ {
		time.Sleep(3 * time.Second)
		pollReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v13/deployments/%s", VercelAPI, deployData.ID), nil)
		pollReq.Header.Set("Authorization", "Bearer "+token)
		pollRes, err := s.httpClient.Do(pollReq)
		if err == nil {
			var pollData struct {
				ReadyState string `json:"readyState"`
				URL        string `json:"url"`
			}
			json.NewDecoder(pollRes.Body).Decode(&pollData)
			pollRes.Body.Close()
			if pollData.ReadyState == "READY" {
				finalURL = "https://" + pollData.URL
				break
			}
		}
	}

	if finalURL == "" {
		finalURL = "https://" + deployData.URL
	}

	patchReq, _ := http.NewRequest(http.MethodPatch, fmt.Sprintf("%s/v9/projects/%s", VercelAPI, projectName), strings.NewReader(`{"ssoProtection":null}`))
	patchReq.Header.Set("Authorization", "Bearer "+token)
	patchReq.Header.Set("Content-Type", "application/json")
	if patchRes, err := s.httpClient.Do(patchReq); err == nil {
		patchRes.Body.Close()
	}

	s.db.AddProxy(projectName, finalURL)
	s.pool.SyncFromDB(s.db)

	s.hub.Broadcast("proxy_added", map[string]string{"url": finalURL, "name": projectName})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ready",
		"name":   projectName,
		"url":    finalURL,
	})
}

// ----------------------------------------------------
// Export & Backup Database (JSON)
// ----------------------------------------------------
// ----------------------------------------------------
// Export & Backup Database (JSON)
// ----------------------------------------------------
func (s *RouterServer) HandleAPIExportBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.requireAuth(w, r) {
		return
	}

	proxies, _ := s.db.GetAllProxies()
	keys, _ := s.db.GetAPIKeys()
	providers, _ := s.db.GetProviders()
	strategy := s.db.GetSetting("strategy", "round-robin")

	backup := map[string]interface{}{
		"version":     "2.0.0",
		"exported_at": time.Now().UTC().Format(time.RFC3339),
		"strategy":    strategy,
		"providers":   providers,
		"nodes":       proxies,
		"proxies":     proxies,
		"relays":      proxies,
		"api_keys":    keys,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="zyrouter-backup-%s.json"`, time.Now().Format("2006-01-02-150405")))
	json.NewEncoder(w).Encode(backup)
}

// ----------------------------------------------------
// Import & Restore Database (JSON)
// ----------------------------------------------------
func (s *RouterServer) HandleAPIImportBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.requireAuth(w, r) {
		return
	}

	var data struct {
		Strategy  string     `json:"strategy"`
		Providers []Provider `json:"providers"`
		Nodes     []struct {
			Name string `json:"name"`
			URL  string `json:"url"`
		} `json:"nodes"`
		Proxies []struct {
			Name string `json:"name"`
			URL  string `json:"url"`
		} `json:"proxies"`
		Relays []struct {
			Name string `json:"name"`
			URL  string `json:"url"`
		} `json:"relays"`
		APIKeys []struct {
			Key           string `json:"key"`
			Name          string `json:"name"`
			AllowedModels string `json:"allowed_models"`
		} `json:"api_keys"`
	}

	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Invalid JSON backup: %v"}`, err), http.StatusBadRequest)
		return
	}

	// 1. Restore Providers
	importedProviders := 0
	for _, p := range data.Providers {
		p.Name = strings.TrimSpace(p.Name)
		p.BaseURL = strings.TrimSpace(p.BaseURL)
		if p.Name != "" && p.BaseURL != "" {
			if err := s.db.SaveProvider(&p); err == nil {
				importedProviders++
			}
		}
	}

	// 2. Restore Relays / Proxies / Nodes
	importedProxies := 0
	allNodes := append(data.Proxies, data.Nodes...)
	allNodes = append(allNodes, data.Relays...)
	seenURLs := make(map[string]bool)

	for _, p := range allNodes {
		urlStr := strings.TrimSpace(p.URL)
		if urlStr != "" && !seenURLs[urlStr] {
			seenURLs[urlStr] = true
			name := strings.TrimSpace(p.Name)
			if name == "" {
				name = "Imported Relay"
			}
			if _, err := s.db.AddProxy(name, urlStr); err == nil {
				importedProxies++
			}
		}
	}

	// 3. Restore Client API Keys
	importedKeys := 0
	for _, k := range data.APIKeys {
		keyStr := strings.TrimSpace(k.Key)
		name := strings.TrimSpace(k.Name)
		allowed := strings.TrimSpace(k.AllowedModels)
		if allowed == "" {
			allowed = "*"
		}
		if keyStr != "" {
			if name == "" {
				name = "Imported Key"
			}
			_, _ = s.db.db.Exec("INSERT OR REPLACE INTO api_keys (id, key, name, allowed_models, is_active, total_requests, total_tokens) VALUES (?, ?, ?, ?, 1, 0, 0)",
				fmt.Sprintf("key_%s", genRandomHex(6)), keyStr, name, allowed)
			importedKeys++
		}
	}

	if data.Strategy != "" {
		s.db.SetSetting("strategy", data.Strategy)
		s.pool.SetStrategy(data.Strategy)
	}

	_ = s.pool.SyncFromDB(s.db)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":            true,
		"imported_providers": importedProviders,
		"imported_proxies":   importedProxies,
		"imported_keys":      importedKeys,
		"total_proxies":      s.pool.TotalNodes(),
	})
}

// ----------------------------------------------------
// Vercel Auto-Sync All Deployments
// ----------------------------------------------------
func (s *RouterServer) HandleAPIVercelSyncAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.requireAuth(w, r) {
		return
	}

	var req struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	token := strings.TrimSpace(req.Token)
	if token == "" {
		http.Error(w, `{"error":"Vercel API token is required"}`, http.StatusBadRequest)
		return
	}

	// Query Vercel deployments API
	vercelURL := "https://api.vercel.com/v6/deployments?limit=100"
	vReq, _ := http.NewRequest(http.MethodGet, vercelURL, nil)
	vReq.Header.Set("Authorization", "Bearer "+token)

	vRes, err := s.httpClient.Do(vReq)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Failed to connect to Vercel API: %v"}`, err), http.StatusInternalServerError)
		return
	}
	defer vRes.Body.Close()

	if vRes.StatusCode >= 400 {
		b, _ := io.ReadAll(vRes.Body)
		http.Error(w, fmt.Sprintf(`{"error":"Vercel API error: %s"}`, string(b)), http.StatusBadRequest)
		return
	}

	var data struct {
		Deployments []struct {
			Name  string `json:"name"`
			URL   string `json:"url"`
			State string `json:"state"`
		} `json:"deployments"`
	}
	if err := json.NewDecoder(vRes.Body).Decode(&data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Failed to parse Vercel response: %v"}`, err), http.StatusInternalServerError)
		return
	}

	addedCount := 0
	for _, d := range data.Deployments {
		if d.URL != "" && (d.State == "READY" || d.State == "BUILDING" || d.State == "") {
			fullURL := "https://" + d.URL
			name := d.Name
			if name == "" {
				name = "Vercel Node"
			}
			if _, err := s.db.AddProxy(name, fullURL); err == nil {
				addedCount++
			}
		}
	}

	_ = s.pool.SyncFromDB(s.db)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":           true,
		"found_deployments": len(data.Deployments),
		"imported_new":      addedCount,
		"total_proxies":     s.pool.TotalNodes(),
	})
}

// ----------------------------------------------------
// OpenAI Endpoints (/v1/...) with API Key Enforcement
// ----------------------------------------------------
func (s *RouterServer) HandleV1Root(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.URL.Path != "/v1" && r.URL.Path != "/v1/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"message": "OpenCode Zen High-Speed AI Gateway (OpenAI Compatible)",
		"version": "1.0.0",
		"endpoints": map[string]string{
			"chat_completions": "/v1/chat/completions",
			"models":           "/v1/models",
			"dashboard":        "/dashboard",
			"health":           "/health",
		},
		"active_proxies": s.pool.TotalNodes(),
		"strategy":       s.pool.GetStrategy(),
	})
}

func (s *RouterServer) HandleHealth(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	stats, _ := s.db.GetGlobalStats()

	resp := map[string]interface{}{
		"status":            "ok",
		"service":           "OpenCode Zen Go Router",
		"database":          "SQLite (router.db)",
		"strategy":          s.pool.GetStrategy(),
		"in_flight":         s.pool.inFlight.Load(),
		"uptime_seconds":    int(time.Since(s.startTime).Seconds()),
		"active_proxies":    s.pool.TotalNodes(),
		"total_routed":      s.pool.totalRouted.Load(),
		"total_retries":     s.pool.totalRetries.Load(),
		"total_tokens":      stats.TotalTokens,
		"prompt_tokens":     stats.PromptTokens,
		"completion_tokens": stats.OutputTokens,
		"endpoints": map[string]string{
			"dashboard": "/",
			"events":    "/api/events",
			"chat":      "/v1/chat/completions",
			"models":    "/v1/models",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *RouterServer) HandleModels(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Validate API Key if client provided one
	authKey := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	valid, keyObj := s.db.ValidateAPIKey(authKey)
	if !valid {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":{"message":"Invalid or inactive API key. Generate one in Zen Router dashboard.","type":"invalid_request_error","code":"invalid_api_key"}}`))
		return
	}

	modelMap := make(map[string]map[string]interface{})

	// 1. Fetch from all active providers
	activeProviders, _ := s.db.GetActiveProviders()
	defaultOpenCodeModels := []string{"mimo-v2.5-free", "glm-4-flash-free", "deepseek-r1-free", "qwen-2.5-coder-32b-free", "nemotron-70b-free"}

	for _, p := range activeProviders {
		prefix := p.ModelPrefix
		if prefix == "" {
			prefix = p.ID
		}

		if p.ProviderType == "opencode" {
			for _, m := range defaultOpenCodeModels {
				// Prefixed model ID (e.g. "opencode/mimo-v2.5-free")
				prefixedID := fmt.Sprintf("%s/%s", prefix, m)
				modelMap[prefixedID] = map[string]interface{}{
					"id":       prefixedID,
					"object":   "model",
					"created":  time.Now().Unix(),
					"owned_by": p.Name,
				}
				// Direct bare alias (e.g. "mimo-v2.5-free")
				if _, exists := modelMap[m]; !exists {
					modelMap[m] = map[string]interface{}{
						"id":       m,
						"object":   "model",
						"created":  time.Now().Unix(),
						"owned_by": p.Name,
					}
				}
			}
		} else if p.BaseURL != "" {
			targetURL := strings.TrimRight(p.BaseURL, "/") + "/models"
			req, err := http.NewRequest(http.MethodGet, targetURL, nil)
			if err == nil {
				keyPool := s.GetKeyPoolForProvider(&p)
				apiKey, _ := keyPool.PickKey()
				if apiKey == "" {
					apiKey = p.APIKey
				}
				req.Header.Set("Authorization", "Bearer "+apiKey)
				client := &http.Client{Timeout: 5 * time.Second}
				resp, err := client.Do(req)
				if err == nil && resp.StatusCode == 200 {
					var pModels struct {
						Data []map[string]interface{} `json:"data"`
					}
					if err := json.NewDecoder(resp.Body).Decode(&pModels); err == nil {
						for _, item := range pModels.Data {
							if id, ok := item["id"].(string); ok && id != "" {
								// Prefixed model ID (e.g. "genspark/gpt-5")
								prefixedID := fmt.Sprintf("%s/%s", prefix, id)
								modelMap[prefixedID] = map[string]interface{}{
									"id":       prefixedID,
									"object":   "model",
									"created":  time.Now().Unix(),
									"owned_by": p.Name,
								}
								// Direct bare alias
								if _, exists := modelMap[id]; !exists {
									item["owned_by"] = p.Name
									modelMap[id] = item
								}
							}
						}
					}
					resp.Body.Close()
				}
			}
		}
	}

	// Filter models based on API Key permissions
	finalList := make([]map[string]interface{}, 0)
	for _, item := range modelMap {
		mID, _ := item["id"].(string)
		if keyObj == nil || keyObj.IsModelAllowed(mID) {
			finalList = append(finalList, item)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"object": "list",
		"data":   finalList,
	})
}

func (s *RouterServer) HandleChatCompletions(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// API Key Validation (like 9router ori)
	authKey := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	valid, keyObj := s.db.ValidateAPIKey(authKey)
	if !valid {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":{"message":"Invalid or inactive API key. Generate one in Zen Router dashboard.","type":"invalid_request_error","code":"invalid_api_key"}}`))
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"Failed to read request body"}`, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	sessionID := extractSessionID(r, bodyBytes)
	isStream := bytes.Contains(bodyBytes, []byte(`"stream":true`)) || bytes.Contains(bodyBytes, []byte(`"stream": true`))

	var reqBody map[string]interface{}
	json.Unmarshal(bodyBytes, &reqBody)
	modelName, _ := reqBody["model"].(string)
	if modelName == "" {
		modelName = "unknown"
	}

	// Model Permission Enforcement per API Key
	if keyObj != nil && !keyObj.IsModelAllowed(modelName) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		errResp := fmt.Sprintf(`{"error":{"message":"Model '%s' is not permitted for this API Key. Allowed models: [%s]","type":"permission_error","code":"model_not_allowed"}}`, modelName, keyObj.AllowedModels)
		w.Write([]byte(errResp))
		return
	}

	// 1. Check Prompt Cache (0ms Instant Return)
	cacheKey := fmt.Sprintf("%x", sha256.Sum256(bodyBytes))
	if cachedBody, cachedHeader, hit := s.promptCache.Get(cacheKey); hit {
		log.Printf("[Cache HIT] Returning instant 0ms cached response for %s", modelName)
		for k, v := range cachedHeader {
			w.Header()[k] = v
		}
		w.Header().Set("x-router-cache", "HIT")
		w.WriteHeader(http.StatusOK)
		w.Write(cachedBody)
		return
	}

	// 2. Multi-Provider Router Resolution & Model Prefix Stripping
	provider, cleanModelName := s.ResolveProviderAndCleanModel(modelName)

	// Update payload model to cleanModelName so upstream gets exact model without prefix
	if cleanModelName != modelName {
		reqBody["model"] = cleanModelName
		bodyBytes, _ = json.Marshal(reqBody)
	}

	if provider != nil && provider.ProviderType != "opencode" {
		s.handleDirectProviderCompletion(w, r, provider, cleanModelName, bodyBytes, isStream, keyObj)
		return
	}

	// 3. Strict Model Fidelity (Keep exact model requested by developer)
	s.pool.inFlight.Add(1)
	defer s.pool.inFlight.Add(-1)

	s.hub.Broadcast("request_start", map[string]interface{}{
		"model":     cleanModelName,
		"is_stream": isStream,
		"in_flight": s.pool.inFlight.Load(),
	})

	var lastErr error
	for attempt := 0; attempt < MaxRetries; attempt++ {
		node, _ := s.pool.Pick(sessionID)
		if node == nil {
			http.Error(w, `{"error":"No proxy available in pool"}`, http.StatusServiceUnavailable)
			return
		}

		node.InFlight.Add(1)

		targetURL := fmt.Sprintf("%s/v1/chat/completions", node.RawURL)
		req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
		if err != nil {
			node.InFlight.Add(-1)
			lastErr = err
			continue
		}

		for k, v := range r.Header {
			req.Header[k] = v
		}

		req.Header.Set("Authorization", "Bearer public")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-opencode-client", "desktop")
		if sessionID != "" {
			req.Header.Set("x-opencode-session", fmt.Sprintf("ses_%s", sessionID))
		} else {
			req.Header.Set("x-opencode-session", fmt.Sprintf("ses_%s", genRandomHex(16)))
		}
		req.Header.Set("x-opencode-request", fmt.Sprintf("msg_%s", genRandomHex(16)))

		t0 := time.Now()
		resp, err := s.httpClient.Do(req)
		if err != nil {
			node.InFlight.Add(-1)
			cd := node.SetCooldownForStatus(500)
			log.Printf("[Retry #%d] Proxy %s failed: %v, cooling down %v and retrying...", attempt+1, node.RawURL, err, cd)
			s.pool.totalRetries.Add(1)
			s.hub.Broadcast("proxy_cooldown", map[string]interface{}{"url": node.RawURL, "cooldown": int(cd.Seconds())})
			lastErr = err
			time.Sleep(time.Duration(150+attempt*50) * time.Millisecond)
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusNotFound || resp.StatusCode >= 500 {
			resp.Body.Close()
			node.InFlight.Add(-1)
			cd := node.SetCooldownForStatus(resp.StatusCode)
			log.Printf("[Retry #%d] Proxy %s returned HTTP %d, cooling down %v and trying next proxy...", attempt+1, node.RawURL, resp.StatusCode, cd)
			s.pool.totalRetries.Add(1)
			go s.db.UpdateProxyStatus(node.RawURL, resp.StatusCode, int(time.Since(t0).Milliseconds()), false)
			s.hub.Broadcast("proxy_cooldown", map[string]interface{}{"url": node.RawURL, "cooldown": int(cd.Seconds()), "status": resp.StatusCode})
			lastErr = fmt.Errorf("upstream returned %d", resp.StatusCode)
			time.Sleep(time.Duration(150+attempt*50) * time.Millisecond)
			continue
		}

		// SUCCESS!
		defer node.InFlight.Add(-1)

		s.pool.totalRouted.Add(1)
		duration := time.Since(t0)
		latencyMs := int(duration.Milliseconds())
		log.Printf("[Routed] %s -> %s (%d OK) [%v] (Model: %s)", r.RemoteAddr, node.RawURL, resp.StatusCode, duration, cleanModelName)

		go s.db.UpdateProxyStatus(node.RawURL, resp.StatusCode, latencyMs, true)

		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		if isStream {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			w.Header().Set("X-Accel-Buffering", "no")
		}
		w.Header().Set("x-router-proxy", node.RawURL)
		w.Header().Set("x-router-cache", "MISS")
		w.WriteHeader(resp.StatusCode)

		promptTokens := 0
		completionTokens := 0
		totalTokens := 0

		if isStream {
			flusher, canFlush := w.(http.Flusher)
			buf := make([]byte, 1024)
			var streamBuffer bytes.Buffer

			for {
				n, readErr := resp.Body.Read(buf)
				if n > 0 {
					w.Write(buf[:n])
					streamBuffer.Write(buf[:n])
					if canFlush {
						flusher.Flush()
					}
				}
				if readErr != nil {
					break
				}
			}
			resp.Body.Close()

			content := streamBuffer.String()
			if strings.Contains(content, `"usage":`) {
				lines := strings.Split(content, "\n")
				for _, line := range lines {
					if strings.Contains(line, `"usage":{`) {
						var parsed struct {
							Usage struct {
								PromptTokens     int `json:"prompt_tokens"`
								CompletionTokens int `json:"completion_tokens"`
								TotalTokens      int `json:"total_tokens"`
							} `json:"usage"`
						}
						jsonStr := strings.TrimPrefix(line, "data: ")
						if json.Unmarshal([]byte(jsonStr), &parsed) == nil {
							promptTokens = parsed.Usage.PromptTokens
							completionTokens = parsed.Usage.CompletionTokens
							totalTokens = parsed.Usage.TotalTokens
						}
					}
				}
			}

			if totalTokens == 0 {
				completionTokens = len(content) / 4
				promptTokens = len(bodyBytes) / 4
				totalTokens = promptTokens + completionTokens
			}

			keyID := ""
			if keyObj != nil {
				keyID = keyObj.ID
			}
			s.db.LogUsage(cleanModelName, node.RawURL, resp.StatusCode, latencyMs, promptTokens, completionTokens, totalTokens, true, keyID)

			s.hub.Broadcast("request_done", map[string]interface{}{
				"model":      cleanModelName,
				"proxy":      node.RawURL,
				"latency_ms": latencyMs,
				"tokens":     totalTokens,
				"status":     resp.StatusCode,
				"in_flight":  s.pool.inFlight.Load(),
			})

			return
		}

		respBodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Save to PromptCache for instant 0ms responses on future matching prompts
		if len(respBodyBytes) > 0 {
			s.promptCache.Set(cacheKey, respBodyBytes, resp.Header)
		}

		w.Write(respBodyBytes)

		var parsedResp struct {
			Usage struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
				TotalTokens      int `json:"total_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(respBodyBytes, &parsedResp) == nil {
			promptTokens = parsedResp.Usage.PromptTokens
			completionTokens = parsedResp.Usage.CompletionTokens
			totalTokens = parsedResp.Usage.TotalTokens
		}
		if totalTokens == 0 {
			completionTokens = len(respBodyBytes) / 4
			promptTokens = len(bodyBytes) / 4
			totalTokens = promptTokens + completionTokens
		}

		keyID := ""
		if keyObj != nil {
			keyID = keyObj.ID
		}
		s.db.LogUsage(modelName, node.RawURL, resp.StatusCode, latencyMs, promptTokens, completionTokens, totalTokens, false, keyID)

		s.hub.Broadcast("request_done", map[string]interface{}{
			"model":      modelName,
			"proxy":      node.RawURL,
			"latency_ms": latencyMs,
			"tokens":     totalTokens,
			"status":     resp.StatusCode,
			"in_flight":  s.pool.inFlight.Load(),
		})

		return
	}

	// Emergency Fallback: If all proxies in pool failed or returned 404, route directly to OpenCode Zen API
	log.Printf("[Direct OpenCode Fallback] Relays failed (%v), routing directly to OpenCode Zen Gateway for model '%s'...", lastErr, cleanModelName)
	directOpenCodeProvider := &Provider{
		ID:           "opencode_direct",
		Name:         "OpenCode Zen Direct Gateway",
		BaseURL:      "https://opencode.ai/zen/v1",
		APIKey:       "public",
		APIKeysPool:  []string{"public"},
		ProviderType: "openai",
	}
	s.handleDirectProviderCompletion(w, r, directOpenCodeProvider, cleanModelName, bodyBytes, isStream, keyObj)
}

func (s *RouterServer) handleDirectProviderCompletion(w http.ResponseWriter, r *http.Request, provider *Provider, modelName string, bodyBytes []byte, isStream bool, keyObj *APIKey) {
	s.pool.inFlight.Add(1)
	defer s.pool.inFlight.Add(-1)

	s.hub.Broadcast("request_start", map[string]interface{}{
		"model":     modelName,
		"is_stream": isStream,
		"in_flight": s.pool.inFlight.Load(),
	})

	keyPool := s.GetKeyPoolForProvider(provider)
	maxKeyRetries := 3

	var lastResp *http.Response
	var lastErr error
	var chosenKey string
	var totalAttempts int
	var latencyMs int

	targetURL := strings.TrimRight(provider.BaseURL, "/") + "/chat/completions"

	for attempt := 0; attempt < maxKeyRetries; attempt++ {
		totalAttempts++
		apiKey, poolSize := keyPool.PickKey()
		chosenKey = apiKey
		if apiKey == "" {
			apiKey = provider.APIKey
		}

		req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		if strings.Contains(provider.BaseURL, "opencode.ai") {
			req.Header.Set("User-Agent", "opencode")
			req.Header.Set("x-opencode-client", "desktop")
			req.Header.Set("x-opencode-session", fmt.Sprintf("ses_%s", genRandomHex(16)))
			req.Header.Set("x-opencode-request", fmt.Sprintf("msg_%s", genRandomHex(16)))
		}
		if isStream {
			req.Header.Set("Accept", "text/event-stream")
		} else {
			req.Header.Set("Accept", "application/json")
		}

		attemptT0 := time.Now()
		resp, err := s.httpClient.Do(req)
		if err != nil {
			lastErr = err
			keyPool.MarkCooldown(apiKey, 15*time.Second)
			log.Printf("[Key Failover #%d] Key in %s failed with network error: %v, trying next key...", attempt+1, provider.Name, err)
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusUnauthorized || resp.StatusCode >= 500 {
			resp.Body.Close()
			keyPool.MarkCooldown(apiKey, 30*time.Second)
			log.Printf("[Key Failover #%d] Key in %s returned HTTP %d, cooling down and trying next key in pool (size: %d)...", attempt+1, provider.Name, resp.StatusCode, poolSize)
			if poolSize > 1 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			lastErr = fmt.Errorf("upstream key returned HTTP %d", resp.StatusCode)
			break
		}

		lastResp = resp
		latencyMs = int(time.Since(attemptT0).Milliseconds())
		lastErr = nil
		break
	}

	if lastResp == nil {
		if lastErr != nil {
			http.Error(w, fmt.Sprintf(`{"error":"Provider %s failed across key pool: %v"}`, provider.Name, lastErr), http.StatusBadGateway)
		} else {
			http.Error(w, fmt.Sprintf(`{"error":"Provider %s unavailable"}`, provider.Name), http.StatusBadGateway)
		}
		return
	}
	defer lastResp.Body.Close()

	resp := lastResp
	s.pool.totalRouted.Add(1)
	maskedKey := chosenKey
	if len(chosenKey) > 10 {
		maskedKey = chosenKey[:6] + "..." + chosenKey[len(chosenKey)-3:]
	}
	log.Printf("[Provider Routed] %s -> %s [Key: %s] (%d OK) (Model: %s)", r.RemoteAddr, provider.Name, maskedKey, resp.StatusCode, modelName)

	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	if isStream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
	}
	w.Header().Set("x-router-provider", provider.Name)
	w.Header().Set("x-router-cache", "MISS")
	w.WriteHeader(resp.StatusCode)

	keyID := ""
	if keyObj != nil {
		keyID = keyObj.ID
	}

	promptTokens := 0
	completionTokens := 0
	totalTokens := 0

	if isStream {
		flusher, canFlush := w.(http.Flusher)
		buf := make([]byte, 1024)
		var streamBuffer bytes.Buffer

		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				w.Write(buf[:n])
				streamBuffer.Write(buf[:n])
				if canFlush {
					flusher.Flush()
				}
			}
			if readErr != nil {
				break
			}
		}

		rawStream := streamBuffer.String()
		lines := strings.Split(rawStream, "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "data: ") && !strings.Contains(line, "[DONE]") {
				var parsed struct {
					Usage struct {
						PromptTokens     int `json:"prompt_tokens"`
						CompletionTokens int `json:"completion_tokens"`
						TotalTokens      int `json:"total_tokens"`
					} `json:"usage"`
				}
				jsonStr := strings.TrimPrefix(line, "data: ")
				if json.Unmarshal([]byte(jsonStr), &parsed) == nil && parsed.Usage.TotalTokens > 0 {
					promptTokens = parsed.Usage.PromptTokens
					completionTokens = parsed.Usage.CompletionTokens
					totalTokens = parsed.Usage.TotalTokens
				}
			}
		}

		if totalTokens == 0 {
			completionTokens = len(rawStream) / 4
			promptTokens = len(bodyBytes) / 4
			totalTokens = promptTokens + completionTokens
		}
	} else {
		respBodyBytes, _ := io.ReadAll(resp.Body)
		w.Write(respBodyBytes)

		var parsedResp struct {
			Usage struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
				TotalTokens      int `json:"total_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(respBodyBytes, &parsedResp) == nil && parsedResp.Usage.TotalTokens > 0 {
			promptTokens = parsedResp.Usage.PromptTokens
			completionTokens = parsedResp.Usage.CompletionTokens
			totalTokens = parsedResp.Usage.TotalTokens
		} else {
			completionTokens = len(respBodyBytes) / 4
			promptTokens = len(bodyBytes) / 4
			totalTokens = promptTokens + completionTokens
		}

		if resp.StatusCode == http.StatusOK {
			cacheKey := fmt.Sprintf("%x", sha256.Sum256(bodyBytes))
			s.promptCache.Set(cacheKey, respBodyBytes, w.Header())
		}
	}

	s.db.LogUsage(modelName, provider.Name, resp.StatusCode, latencyMs, promptTokens, completionTokens, totalTokens, isStream, keyID)

	s.hub.Broadcast("request_done", map[string]interface{}{
		"model":      modelName,
		"proxy":      provider.Name,
		"latency_ms": latencyMs,
		"tokens":     totalTokens,
		"status":     resp.StatusCode,
		"in_flight":  s.pool.inFlight.Load(),
	})
}

// GetDefaultDBPath resolves the database path matching 9router ori (%APPDATA%\zyrouter or ~/.zyrouter), named zyrouter.db
func GetDefaultDBPath() string {
	if explicit := os.Getenv("DB_FILE"); explicit != "" {
		return explicit
	}

	appName := "zyrouter"
	var baseDir string

	if customDataDir := os.Getenv("DATA_DIR"); customDataDir != "" {
		baseDir = customDataDir
	} else if runtime.GOOS == "windows" {
		appData := os.Getenv("APPDATA")
		if appData != "" {
			baseDir = filepath.Join(appData, appName)
		} else {
			home, _ := os.UserHomeDir()
			baseDir = filepath.Join(home, "AppData", "Roaming", appName)
		}
	} else {
		home, _ := os.UserHomeDir()
		baseDir = filepath.Join(home, "."+appName)
	}

	// Ensure directory exists
	_ = os.MkdirAll(baseDir, 0755)

	targetDB := filepath.Join(baseDir, "zyrouter.db")

	// Auto-migrate legacy router.db if present in local directory
	if _, err := os.Stat("router.db"); err == nil {
		if _, err := os.Stat(targetDB); os.IsNotExist(err) {
			log.Printf("[Migration] Migrating local router.db to %s", targetDB)
			data, readErr := os.ReadFile("router.db")
			if readErr == nil {
				_ = os.WriteFile(targetDB, data, 0644)
			}
		}
	}

	return targetDB
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = DefaultPort
	}

	dbFile := GetDefaultDBPath()

	db, err := InitDB(dbFile)
	if err != nil {
		log.Fatalf("Failed to initialize SQLite: %v", err)
	}

	pool := NewProxyPool()
	if err := pool.SyncFromDB(db); err != nil {
		log.Printf("[Warn] Failed to sync from DB: %v", err)
	}
	pool.SetStrategy(db.GetSetting("strategy", "round-robin"))

	// Periodic DB sync so newly deployed nodes are auto-discovered without restarting
	go func() {
		for {
			time.Sleep(10 * time.Second)
			_ = pool.SyncFromDB(db)
		}
	}()

	hub := NewEventHub()
	server := NewRouterServer(db, pool, hub)

	mux := http.NewServeMux()
	// Web Dashboard
	mux.HandleFunc("/", server.HandleDashboard)
	mux.HandleFunc("/dashboard", server.HandleDashboard)

	// Authentication API (Bcrypt Hashed)
	mux.HandleFunc("/api/auth/login", server.HandleAPILogin)
	mux.HandleFunc("/api/auth/logout", server.HandleAPILogout)
	mux.HandleFunc("/api/auth/me", server.HandleAPIAuthMe)
	mux.HandleFunc("/api/auth/password", server.HandleAPIChangePassword)

	// API Key Management API
	mux.HandleFunc("/api/keys", server.HandleAPIKeys)
	mux.HandleFunc("/api/keys/toggle", server.HandleAPIToggleKey)
	mux.HandleFunc("/api/keys/update-models", server.HandleAPIUpdateKeyModels)

	// Multi-Provider Management API
	mux.HandleFunc("/api/providers", server.HandleAPIProviders)
	mux.HandleFunc("/api/providers/toggle", server.HandleAPIToggleProvider)

	// Live Event Stream for Dashboard
	mux.HandleFunc("/api/events", server.HandleLiveEvents)
	mux.HandleFunc("/usage/stream", server.HandleLiveEvents)

	// Admin API
	mux.HandleFunc("/api/proxies", server.HandleAPIProxies)
	mux.HandleFunc("/api/proxies/toggle", server.HandleAPIToggleProxy)
	mux.HandleFunc("/api/proxies/ping", server.HandleAPIPingProxy)
	mux.HandleFunc("/api/strategy", server.HandleAPIStrategy)
	mux.HandleFunc("/api/logs", server.HandleAPILogs)
	mux.HandleFunc("/api/deploy-vercel", server.HandleAPIDeployVercel)
	mux.HandleFunc("/api/backup/export", server.HandleAPIExportBackup)
	mux.HandleFunc("/api/backup/import", server.HandleAPIImportBackup)
	mux.HandleFunc("/api/vercel/sync-all", server.HandleAPIVercelSyncAll)

	// OpenAI API Endpoints
	mux.HandleFunc("/v1", server.HandleV1Root)
	mux.HandleFunc("/v1/", server.HandleV1Root)
	mux.HandleFunc("/health", server.HandleHealth)
	mux.HandleFunc("/v1/models", server.HandleModels)
	mux.HandleFunc("/v1/chat/completions", server.HandleChatCompletions)

	host := os.Getenv("HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	addr := host + ":" + port
	log.Printf("==========================================================")
	log.Printf("🚀 OpenCode Zen Go Router (Enterprise Analytics & Auth)")
	log.Printf("📊 Web Dashboard:    http://%s/dashboard", addr)
	log.Printf("🎯 OpenAI Endpoint:   http://%s/v1", addr)
	log.Printf("⚡ Live SSE Events:   http://%s/api/events", addr)
	log.Printf("🔑 API Key Security: Enabled (like 9router ori)")
	log.Printf("🔒 Password Hashing: Bcrypt Active")
	log.Printf("💾 SQLite Database:   %s", dbFile)
	log.Printf("📦 Active Proxies:    %d", pool.TotalNodes())
	log.Printf("==========================================================")

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server stopped: %v", err)
	}
}
