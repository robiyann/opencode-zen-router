# 🚀 OpenCode Zen Go Router (Enterprise Multi-Provider & Auth)

Router AI Gateway berkecepatan tinggi dengan arsitektur **Multi-Provider (Ala 9router)** yang menggabungkan armada **284 Vercel Edge Relays** (OpenCode Free Models) dan **Genspark AI Gateway** (53+ Premium Frontier Models) ke dalam 1 endpoint router terpadu. Ditulis murni dengan **Golang**, menggunakan **SQLite WAL** untuk persistensi data, dilengkapi **Custom Model Prefix**, **Upstream Multi-Key Round-Robin & Auto-Failover**, **Granular Model-Level API Key Access Control**, dan **Web Dashboard Modern** yang di-embed langsung ke dalam single binary!

---

## ✨ Fitur Unggulan

* 🌐 **Multi-Provider Engine (Ala 9router)**:
  * **Provider 1: OpenCode Zen Fleet** $\rightarrow$ Load balancing across 284 active serverless edge relays (`mimo-v2.5-free`, `glm-4-flash-free`, `deepseek-r1-free`, `qwen-2.5-coder-32b-free`, dll).
  * **Provider 2: Genspark AI Gateway** $\rightarrow$ Direct LLM proxy gateway untuk 53+ premium models (`gpt-5`, `claude-sonnet-4-6`, `claude-opus-4-6`, `deep-seek-v4-pro`, `kimi-k2p5`, `grok-4.5`, dll).
* 🏷️ **Custom Model Prefix per Provider**:
  * Panggil model dengan prefix: `genspark/gpt-5`, `opencode/mimo-v2.5-free`, atau `gs/claude-sonnet-4-6`.
  * Router otomatis memotong prefix sebelum diteruskan ke upstream provider.
* ⚡ **Upstream Multi-Key Round-Robin & 429 Auto-Failover**:
  * Simpan banyak upstream API key sekaligus per provider.
  * Request berotasi secara round-robin. Jika salah satu key terkena limit 429 atau 401, key tersebut otomatis masuk cooldown dan request langsung dialihkan ke key berikutnya tanpa menggagalkan client.
* 🔑 **Granular API Key Management**:
  * Generate client API Key unik `sk-zen-...` dengan pembatasan model spesifik (`genspark/*`, `opencode/*`, atau model tertentu).
* 🔒 **Admin Authentication (Bcrypt Hashed)**:
  * Proteksi dashboard login dengan hashing Bcrypt. Password default: `admin123` (bisa diubah langsung di dashboard).
* 📊 **Enterprise Live Dashboard & Analytics**:
  * Concurrency meter, token accounting (prompt, output, total), moving latency charts, relay node topology, dan live activity feed.

---

## 💻 Cara Menjalankan Lokal

```powershell
# Jalankan di Windows
.\start-router.bat
```
👉 Buka Dashboard: **`http://127.0.0.1:8080/dashboard`** (Password: `admin123`)

---

## 🐧 Cara Deploy ke Linux VPS (Ubuntu / Debian / CentOS)

### Opsi 1: Build & Jalankan Langsung dari Source
```bash
# 1. Clone repository
git clone https://github.com/robiyann/opencode-zen-router.git
cd opencode-zen-router

# 2. Build binary
go build -o opencode-router .

# 3. Jalankan binary
HOST=0.0.0.0 PORT=8080 ./opencode-router
```

---

### Opsi 2: Deploy sebagai Systemd Service (Background Selamanya + Auto-Restart)

1. Pindahkan binary ke `/usr/local/bin`:
   ```bash
   sudo cp opencode-router /usr/local/bin/opencode-router
   sudo chmod +x /usr/local/bin/opencode-router
   ```

2. Buat file service systemd:
   ```bash
   sudo nano /etc/systemd/system/opencode-router.service
   ```

3. Paste konfigurasi berikut:
   ```ini
   [Unit]
   Description=OpenCode Zen Multi-Provider AI Router
   After=network.target

   [Service]
   Type=simple
   User=root
   WorkingDirectory=/root
   ExecStart=/usr/local/bin/opencode-router
   Restart=always
   RestartSec=5
   Environment=HOST=0.0.0.0
   Environment=PORT=8080
   # Opsional: Pasang Default Genspark Key via Env
   # Environment=GENSPARK_API_KEY=gsk-...

   [Install]
   WantedBy=multi-user.target
   ```

4. Aktifkan dan jalankan service:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable opencode-router
   sudo systemctl start opencode-router
   sudo systemctl status opencode-router
   ```

5. Buka firewall port 8080 (jika menggunakan UFW):
   ```bash
   sudo ufw allow 8080/tcp
   ```

---

## ⚙️ Integrasi Client & IDE (Cursor, VS Code, Roo Code, Cline)

* **OpenAI Base URL**: `http://IP_VPS_KAMU:8080/v1`
* **API Key**: `sk-zen-...` (Buat di tab **API Keys Management** pada dashboard)
* **Model Examples**:
  * `genspark/gpt-5`
  * `genspark/claude-sonnet-4-6`
  * `genspark/deep-seek-v4-pro`
  * `opencode/mimo-v2.5-free`
  * `opencode/glm-4-flash-free`

