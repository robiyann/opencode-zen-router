package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

type DBProxy struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	URL          string `json:"url"`
	IsActive     bool   `json:"is_active"`
	LatencyMs    int    `json:"latency_ms"`
	LastStatus   int    `json:"last_status"`
	SuccessCount int    `json:"success_count"`
	ErrorCount   int    `json:"error_count"`
	CreatedAt    string `json:"created_at"`
}

type Provider struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	ProviderType string   `json:"provider_type"` // 'opencode', 'genspark', 'openai'
	BaseURL      string   `json:"base_url"`
	APIKey       string   `json:"api_key"`
	APIKeysPool  []string `json:"api_keys_pool"` // Multi-key rotation pool
	ModelPrefix  string   `json:"model_prefix"`  // e.g. "genspark", "opencode", "gs"
	MaskedKey    string   `json:"masked_key"`
	IsActive     bool     `json:"is_active"`
	Models       string   `json:"models"` // '*' or comma-separated list
	CreatedAt    string   `json:"created_at"`
}

type APIKey struct {
	ID            string `json:"id"`
	Key           string `json:"key"`
	MaskedKey     string `json:"masked_key"`
	Name          string `json:"name"`
	AllowedModels string `json:"allowed_models"`
	IsActive      bool   `json:"is_active"`
	TotalRequests int    `json:"total_requests"`
	TotalTokens   int    `json:"total_tokens"`
	LastUsedAt    string `json:"last_used_at"`
	CreatedAt     string `json:"created_at"`
}

type UsageLog struct {
	ID               int64  `json:"id"`
	Timestamp        string `json:"timestamp"`
	Model            string `json:"model"`
	ProxyURL         string `json:"proxy_url"`
	Status           int    `json:"status"`
	LatencyMs        int    `json:"latency_ms"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	TotalTokens      int    `json:"total_tokens"`
	IsStream         bool   `json:"is_stream"`
}

type GlobalStats struct {
	TotalRequests int64 `json:"total_requests"`
	TotalTokens   int64 `json:"total_tokens"`
	PromptTokens  int64 `json:"prompt_tokens"`
	OutputTokens  int64 `json:"output_tokens"`
}

type UsageLogPayload struct {
	Model            string
	ProxyURL         string
	Status           int
	LatencyMs        int
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
	IsStream         bool
	APIKeyID         string
}

type Database struct {
	db      *sql.DB
	logChan chan *UsageLogPayload
}

func InitDB(filepath string) (*Database, error) {
	db, err := sql.Open("sqlite", filepath)
	if err != nil {
		return nil, err
	}

	// SQLite High-Concurrency Optimizations: WAL mode, Busy Timeout & Cache
	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec("PRAGMA synchronous=NORMAL;")
	db.Exec("PRAGMA busy_timeout=10000;")
	db.Exec("PRAGMA cache_size=10000;")
	db.Exec("PRAGMA temp_store=MEMORY;")

	// Connection Pool Settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS proxies (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		url TEXT NOT NULL UNIQUE,
		is_active BOOLEAN NOT NULL DEFAULT 1,
		latency_ms INTEGER DEFAULT 0,
		last_status INTEGER DEFAULT 200,
		success_count INTEGER DEFAULT 0,
		error_count INTEGER DEFAULT 0,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS api_keys (
		id TEXT PRIMARY KEY,
		key TEXT UNIQUE NOT NULL,
		name TEXT NOT NULL,
		allowed_models TEXT NOT NULL DEFAULT '*',
		is_active BOOLEAN NOT NULL DEFAULT 1,
		total_requests INTEGER DEFAULT 0,
		total_tokens INTEGER DEFAULT 0,
		last_used_at DATETIME,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS sessions (
		token TEXT PRIMARY KEY,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS usage_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		model TEXT NOT NULL,
		proxy_url TEXT NOT NULL,
		status INTEGER NOT NULL,
		latency_ms INTEGER DEFAULT 0,
		prompt_tokens INTEGER DEFAULT 0,
		completion_tokens INTEGER DEFAULT 0,
		total_tokens INTEGER DEFAULT 0,
		is_stream BOOLEAN DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS settings (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS providers (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		provider_type TEXT NOT NULL,
		base_url TEXT NOT NULL,
		api_key TEXT NOT NULL,
		api_keys_pool TEXT DEFAULT '',
		model_prefix TEXT DEFAULT '',
		is_active BOOLEAN NOT NULL DEFAULT 1,
		models TEXT DEFAULT '*',
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	`

	if _, err := db.Exec(createTableQuery); err != nil {
		log.Printf("[DB] Table init error: %v", err)
	}

	db.Exec("ALTER TABLE proxies ADD COLUMN success_count INTEGER DEFAULT 0;")
	db.Exec("ALTER TABLE proxies ADD COLUMN error_count INTEGER DEFAULT 0;")
	db.Exec("ALTER TABLE api_keys ADD COLUMN allowed_models TEXT DEFAULT '*';")
	db.Exec("ALTER TABLE providers ADD COLUMN model_prefix TEXT DEFAULT '';")
	db.Exec("ALTER TABLE providers ADD COLUMN api_keys_pool TEXT DEFAULT '';")

	database := &Database{
		db:      db,
		logChan: make(chan *UsageLogPayload, 20000),
	}

	// Start dedicated async log writer worker to completely eliminate race conditions and lock contention
	go database.startLogWorker()

	// Initialize default settings
	database.SetSetting("strategy", "round-robin")

	// Set default hashed password (default: "admin123") if not exists
	if database.GetSetting("admin_password_hash", "") == "" {
		hashed, _ := bcrypt.GenerateFromPassword([]byte("admin123"), bcrypt.DefaultCost)
		database.SetSetting("admin_password_hash", string(hashed))
		log.Printf("[Auth] Default admin password set: admin123 (Change in settings)")
	}

	// Seed default providers (OpenCode Zen & Genspark AI)
	database.SeedDefaultProviders()

	// Seed default proxy if empty
	var count int
	db.QueryRow("SELECT COUNT(*) FROM proxies").Scan(&count)
	if count == 0 {
		database.AddProxy("Primary Vercel Relay", "https://opencode-vercel-proxy-woad.vercel.app")
	}

	return database, nil
}

// Dedicated single-writer worker: drains logChan sequentially with prepared statements
func (d *Database) startLogWorker() {
	insertLogStmt, err := d.db.Prepare(`INSERT INTO usage_logs (model, proxy_url, status, latency_ms, prompt_tokens, completion_tokens, total_tokens, is_stream) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Printf("[DB Worker] Failed to prepare insert log stmt: %v", err)
	} else {
		defer insertLogStmt.Close()
	}

	updateKeyStmt, err := d.db.Prepare(`UPDATE api_keys SET total_requests = total_requests + 1, total_tokens = total_tokens + ?, last_used_at = CURRENT_TIMESTAMP WHERE id = ?`)
	if err != nil {
		log.Printf("[DB Worker] Failed to prepare update key stmt: %v", err)
	} else {
		defer updateKeyStmt.Close()
	}

	for payload := range d.logChan {
		if insertLogStmt != nil {
			_, err := insertLogStmt.Exec(payload.Model, payload.ProxyURL, payload.Status, payload.LatencyMs, payload.PromptTokens, payload.CompletionTokens, payload.TotalTokens, payload.IsStream)
			if err != nil {
				log.Printf("[DB Worker] Log write error: %v", err)
			}
		}

		if payload.APIKeyID != "" && updateKeyStmt != nil {
			_, err := updateKeyStmt.Exec(payload.TotalTokens, payload.APIKeyID)
			if err != nil {
				log.Printf("[DB Worker] Key update error: %v", err)
			}
		}
	}
}

// ----------------------------------------------------
// Proxy Management
// ----------------------------------------------------
func (d *Database) GetAllProxies() ([]DBProxy, error) {
	rows, err := d.db.Query("SELECT id, name, url, is_active, latency_ms, last_status, success_count, error_count, datetime(created_at) FROM proxies ORDER BY id DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]DBProxy, 0)
	for rows.Next() {
		var p DBProxy
		if err := rows.Scan(&p.ID, &p.Name, &p.URL, &p.IsActive, &p.LatencyMs, &p.LastStatus, &p.SuccessCount, &p.ErrorCount, &p.CreatedAt); err == nil {
			list = append(list, p)
		}
	}
	return list, nil
}

func (d *Database) GetActiveProxies() ([]string, error) {
	rows, err := d.db.Query("SELECT url FROM proxies WHERE is_active = 1")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	urls := make([]string, 0)
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err == nil {
			urls = append(urls, u)
		}
	}
	return urls, nil
}

func (d *Database) AddProxy(name, urlStr string) (int64, error) {
	res, err := d.db.Exec("INSERT OR IGNORE INTO proxies (name, url, is_active) VALUES (?, ?, 1)", name, urlStr)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (d *Database) DeleteProxy(id int64) error {
	_, err := d.db.Exec("DELETE FROM proxies WHERE id = ?", id)
	return err
}

func (d *Database) ToggleProxy(id int64, isActive bool) error {
	_, err := d.db.Exec("UPDATE proxies SET is_active = ? WHERE id = ?", isActive, id)
	return err
}

func (d *Database) UpdateProxyStatus(urlStr string, status int, latencyMs int, isSuccess bool) error {
	var query string
	if isSuccess {
		query = "UPDATE proxies SET last_status = ?, latency_ms = ?, success_count = success_count + 1 WHERE url = ?"
	} else {
		query = "UPDATE proxies SET last_status = ?, latency_ms = ?, error_count = error_count + 1 WHERE url = ?"
	}
	_, err := d.db.Exec(query, status, latencyMs, urlStr)
	return err
}

// ----------------------------------------------------
// API Key Management (Like 9router Ori)
// ----------------------------------------------------
func (d *Database) GetAPIKeys() ([]APIKey, error) {
	rows, err := d.db.Query("SELECT id, key, name, COALESCE(allowed_models, '*'), is_active, total_requests, total_tokens, COALESCE(datetime(last_used_at), '-'), datetime(created_at) FROM api_keys ORDER BY created_at DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]APIKey, 0)
	for rows.Next() {
		var k APIKey
		if err := rows.Scan(&k.ID, &k.Key, &k.Name, &k.AllowedModels, &k.IsActive, &k.TotalRequests, &k.TotalTokens, &k.LastUsedAt, &k.CreatedAt); err == nil {
			if k.AllowedModels == "" {
				k.AllowedModels = "*"
			}
			if len(k.Key) > 12 {
				k.MaskedKey = k.Key[:7] + "..." + k.Key[len(k.Key)-4:]
			} else {
				k.MaskedKey = k.Key
			}
			list = append(list, k)
		}
	}
	return list, nil
}

func (d *Database) CreateAPIKey(name string, allowedModels string) (*APIKey, error) {
	b := make([]byte, 16)
	rand.Read(b)
	id := fmt.Sprintf("key_%s", hex.EncodeToString(b[:6]))
	rawKey := fmt.Sprintf("sk-zen-%s", hex.EncodeToString(b))

	if strings.TrimSpace(allowedModels) == "" {
		allowedModels = "*"
	}

	query := "INSERT INTO api_keys (id, key, name, allowed_models, is_active, total_requests, total_tokens) VALUES (?, ?, ?, ?, 1, 0, 0)"
	_, err := d.db.Exec(query, id, rawKey, name, allowedModels)
	if err != nil {
		return nil, err
	}

	return &APIKey{
		ID:            id,
		Key:           rawKey,
		MaskedKey:     rawKey[:7] + "..." + rawKey[len(rawKey)-4:],
		Name:          name,
		AllowedModels: allowedModels,
		IsActive:      true,
	}, nil
}

func (d *Database) UpdateAPIKeyModels(id string, allowedModels string) error {
	if strings.TrimSpace(allowedModels) == "" {
		allowedModels = "*"
	}
	_, err := d.db.Exec("UPDATE api_keys SET allowed_models = ? WHERE id = ?", allowedModels, id)
	return err
}

func (d *Database) DeleteAPIKey(id string) error {
	_, err := d.db.Exec("DELETE FROM api_keys WHERE id = ?", id)
	return err
}

func (d *Database) ToggleAPIKey(id string, isActive bool) error {
	_, err := d.db.Exec("UPDATE api_keys SET is_active = ? WHERE id = ?", isActive, id)
	return err
}

// ValidateAPIKey checks if key exists. If no keys are registered in DB yet, allows public/any.
func (d *Database) ValidateAPIKey(keyStr string) (bool, *APIKey) {
	var count int
	d.db.QueryRow("SELECT COUNT(*) FROM api_keys").Scan(&count)
	if count == 0 {
		// If user hasn't generated any keys yet, allow public access
		return true, nil
	}

	var k APIKey
	var activeInt int
	err := d.db.QueryRow("SELECT id, key, name, COALESCE(allowed_models, '*'), is_active FROM api_keys WHERE key = ?", keyStr).Scan(&k.ID, &k.Key, &k.Name, &k.AllowedModels, &activeInt)
	if err != nil {
		return false, nil
	}

	if activeInt != 1 {
		return false, nil
	}

	if k.AllowedModels == "" {
		k.AllowedModels = "*"
	}
	k.IsActive = true
	return true, &k
}

func (k *APIKey) IsModelAllowed(modelName string) bool {
	if k == nil {
		return true
	}
	allowed := strings.TrimSpace(k.AllowedModels)
	if allowed == "" || allowed == "*" || strings.ToLower(allowed) == "all" {
		return true
	}

	parts := strings.Split(allowed, ",")
	cleanModel := strings.ToLower(strings.TrimSpace(modelName))

	// If cleanModel has a prefix (e.g. "genspark/gpt-5"), extract bare model ("gpt-5")
	bareModel := cleanModel
	if strings.Contains(cleanModel, "/") {
		sub := strings.SplitN(cleanModel, "/", 2)
		bareModel = sub[1]
	}

	for _, p := range parts {
		cleanPart := strings.ToLower(strings.TrimSpace(p))
		if cleanPart == "*" || cleanPart == "all" || cleanPart == cleanModel || cleanPart == bareModel {
			return true
		}
		// Prefix wildcard support (e.g. "genspark/*" or "opencode/*")
		if strings.HasSuffix(cleanPart, "/*") {
			prefix := strings.TrimSuffix(cleanPart, "/*")
			if strings.HasPrefix(cleanModel, prefix+"/") {
				return true
			}
		}
	}
	return false
}

func (d *Database) RecordAPIKeyUsage(id string, tokens int) {
	d.db.Exec("UPDATE api_keys SET total_requests = total_requests + 1, total_tokens = total_tokens + ?, last_used_at = CURRENT_TIMESTAMP WHERE id = ?", tokens, id)
}

// ----------------------------------------------------
// Password & Session Auth (Bcrypt Hash Like 9router Ori)
// ----------------------------------------------------
func (d *Database) VerifyPassword(plainPassword string) bool {
	hash := d.GetSetting("admin_password_hash", "")
	if hash == "" {
		return plainPassword == "admin123"
	}
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(plainPassword))
	return err == nil
}

func (d *Database) SetPassword(newPassword string) error {
	hashed, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	return d.SetSetting("admin_password_hash", string(hashed))
}

func (d *Database) CreateSession() (string, error) {
	b := make([]byte, 24)
	rand.Read(b)
	token := hex.EncodeToString(b)
	_, err := d.db.Exec("INSERT INTO sessions (token) VALUES (?)", token)
	return token, err
}

func (d *Database) ValidateSession(token string) bool {
	if token == "" {
		return false
	}
	var count int
	d.db.QueryRow("SELECT COUNT(*) FROM sessions WHERE token = ?", token).Scan(&count)
	return count > 0
}

func (d *Database) DeleteSession(token string) {
	d.db.Exec("DELETE FROM sessions WHERE token = ?", token)
}

// ----------------------------------------------------
// Usage Logs & Global Stats
// ----------------------------------------------------
func (d *Database) LogUsage(model, proxyURL string, status, latencyMs, promptTokens, completionTokens, totalTokens int, isStream bool, apiKeyID string) {
	payload := &UsageLogPayload{
		Model:            model,
		ProxyURL:         proxyURL,
		Status:           status,
		LatencyMs:        latencyMs,
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		TotalTokens:      totalTokens,
		IsStream:         isStream,
		APIKeyID:         apiKeyID,
	}

	select {
	case d.logChan <- payload:
	default:
		log.Printf("[DB] Log queue full, dropping metric to maintain maximum throughput")
	}
}

func (d *Database) GetRecentLogs(limit int) ([]UsageLog, error) {
	rows, err := d.db.Query("SELECT id, datetime(timestamp), model, proxy_url, status, latency_ms, prompt_tokens, completion_tokens, total_tokens, is_stream FROM usage_logs ORDER BY id DESC LIMIT ?", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	logs := make([]UsageLog, 0)
	for rows.Next() {
		var l UsageLog
		if err := rows.Scan(&l.ID, &l.Timestamp, &l.Model, &l.ProxyURL, &l.Status, &l.LatencyMs, &l.PromptTokens, &l.CompletionTokens, &l.TotalTokens, &l.IsStream); err == nil {
			logs = append(logs, l)
		}
	}
	return logs, nil
}

func (d *Database) GetGlobalStats() (GlobalStats, error) {
	var s GlobalStats
	row := d.db.QueryRow("SELECT COUNT(*), COALESCE(SUM(total_tokens), 0), COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0) FROM usage_logs")
	row.Scan(&s.TotalRequests, &s.TotalTokens, &s.PromptTokens, &s.OutputTokens)
	return s, nil
}

func (d *Database) GetSetting(key, defVal string) string {
	var val string
	err := d.db.QueryRow("SELECT value FROM settings WHERE key = ?", key).Scan(&val)
	if err != nil || val == "" {
		return defVal
	}
	return val
}

func (d *Database) SetSetting(key, val string) error {
	_, err := d.db.Exec("INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", key, val)
	return err
}

// ----------------------------------------------------
// Multi-Provider Management
// ----------------------------------------------------
func (d *Database) SeedDefaultProviders() {
	d.db.Exec(`INSERT OR IGNORE INTO providers (id, name, provider_type, base_url, api_key, api_keys_pool, model_prefix, is_active, models) 
		VALUES ('opencode', 'OpenCode Zen Fleet', 'opencode', 'https://opencode.ai', 'public', '["public"]', 'opencode', 1, 'mimo-v2.5-free,glm-4-flash-free,deepseek-r1-free,qwen-2.5-coder-32b-free,nemotron-70b-free')`)

	gensparkKey := os.Getenv("GENSPARK_API_KEY")
	gensparkPoolJSON := "[]"
	if gensparkKey != "" {
		b, _ := json.Marshal([]string{gensparkKey})
		gensparkPoolJSON = string(b)
	}

	d.db.Exec(`INSERT OR IGNORE INTO providers (id, name, provider_type, base_url, api_key, api_keys_pool, model_prefix, is_active, models) 
		VALUES ('genspark', 'Genspark AI Gateway', 'genspark', 'https://www.genspark.ai/api/llm_proxy/v1', ?, ?, 'genspark', 1, '*')`, gensparkKey, gensparkPoolJSON)
}

func (d *Database) GetProviders() ([]Provider, error) {
	rows, err := d.db.Query("SELECT id, name, provider_type, base_url, api_key, COALESCE(api_keys_pool, ''), COALESCE(model_prefix, ''), is_active, COALESCE(models, '*'), datetime(created_at) FROM providers ORDER BY created_at ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]Provider, 0)
	for rows.Next() {
		var p Provider
		var rawPool string
		if err := rows.Scan(&p.ID, &p.Name, &p.ProviderType, &p.BaseURL, &p.APIKey, &rawPool, &p.ModelPrefix, &p.IsActive, &p.Models, &p.CreatedAt); err == nil {
			if p.ModelPrefix == "" {
				p.ModelPrefix = p.ID
			}

			// Parse API Keys Pool
			p.APIKeysPool = make([]string, 0)
			if rawPool != "" {
				json.Unmarshal([]byte(rawPool), &p.APIKeysPool)
			}
			if len(p.APIKeysPool) == 0 && p.APIKey != "" {
				// Fallback split by comma or newline if user inputted raw
				for _, line := range strings.Split(strings.ReplaceAll(p.APIKey, "\r\n", "\n"), "\n") {
					for _, k := range strings.Split(line, ",") {
						k = strings.TrimSpace(k)
						if k != "" {
							p.APIKeysPool = append(p.APIKeysPool, k)
						}
					}
				}
			}
			if len(p.APIKeysPool) > 0 {
				p.APIKey = p.APIKeysPool[0]
			}

			// Format Masked Key
			if len(p.APIKeysPool) > 1 {
				firstKey := p.APIKeysPool[0]
				masked := firstKey
				if len(firstKey) > 12 {
					masked = firstKey[:6] + "..." + firstKey[len(firstKey)-3:]
				}
				p.MaskedKey = fmt.Sprintf("%s (%d keys pool)", masked, len(p.APIKeysPool))
			} else if len(p.APIKey) > 12 {
				p.MaskedKey = p.APIKey[:7] + "..." + p.APIKey[len(p.APIKey)-4:]
			} else {
				p.MaskedKey = p.APIKey
			}

			list = append(list, p)
		}
	}
	return list, nil
}

func (d *Database) GetActiveProviders() ([]Provider, error) {
	all, err := d.GetProviders()
	if err != nil {
		return nil, err
	}
	active := make([]Provider, 0)
	for _, p := range all {
		if p.IsActive {
			active = append(active, p)
		}
	}
	return active, nil
}

func (d *Database) SaveProvider(p *Provider) error {
	if p.ID == "" {
		b := make([]byte, 6)
		rand.Read(b)
		p.ID = fmt.Sprintf("prov_%s", hex.EncodeToString(b))
	}
	if p.ModelPrefix == "" {
		p.ModelPrefix = strings.ToLower(strings.ReplaceAll(p.Name, " ", "_"))
	}
	if p.Models == "" {
		p.Models = "*"
	}

	// Normalize api_keys_pool
	if len(p.APIKeysPool) == 0 && p.APIKey != "" {
		for _, line := range strings.Split(strings.ReplaceAll(p.APIKey, "\r\n", "\n"), "\n") {
			for _, k := range strings.Split(line, ",") {
				k = strings.TrimSpace(k)
				if k != "" {
					p.APIKeysPool = append(p.APIKeysPool, k)
				}
			}
		}
	}
	if len(p.APIKeysPool) > 0 {
		p.APIKey = p.APIKeysPool[0]
	}
	poolBytes, _ := json.Marshal(p.APIKeysPool)

	query := `INSERT INTO providers (id, name, provider_type, base_url, api_key, api_keys_pool, model_prefix, is_active, models) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) 
		ON CONFLICT(id) DO UPDATE SET 
			name = excluded.name, 
			provider_type = excluded.provider_type, 
			base_url = excluded.base_url, 
			api_key = excluded.api_key, 
			api_keys_pool = excluded.api_keys_pool, 
			model_prefix = excluded.model_prefix, 
			is_active = excluded.is_active, 
			models = excluded.models`
	_, err := d.db.Exec(query, p.ID, p.Name, p.ProviderType, p.BaseURL, p.APIKey, string(poolBytes), p.ModelPrefix, p.IsActive, p.Models)
	return err
}

func (d *Database) ToggleProvider(id string, isActive bool) error {
	_, err := d.db.Exec("UPDATE providers SET is_active = ? WHERE id = ?", isActive, id)
	return err
}

func (d *Database) DeleteProvider(id string) error {
	_, err := d.db.Exec("DELETE FROM providers WHERE id = ?", id)
	return err
}
