# 🚀 OpenCode Zen Go Router (Enterprise Analytics & Auth)

Router AI Gateway berkecepatan tinggi khusus OpenCode Zen, ditulis dengan **Golang**, menggunakan **SQLite WAL** untuk persistensi data, dilengkapi **Sistem Keamanan API Key**, **Autentikasi Login Admin (Bcrypt Hashed)** persis seperti 9router ori, dan **Web Dashboard Modern** yang di-embed langsung ke dalam single binary!

---

## ✨ Fitur Unggulan

* 🔒 **Admin Authentication (Bcrypt Hashed)**: Dashboard diproteksi dengan hashing password Bcrypt standar industri (`admin_password_hash` tersimpan di SQLite).
  * *Default password awal:* `admin123` (Bisa diubah langsung di dashboard).
* 🔑 **API Key Management (Kek 9router Ori)**:
  * Generate API Key unik format `sk-zen-...` untuk client AI Agent (Cursor, Cline, Roo Code, Python).
  * Enforced Authorization: Jika ada API Key di database, semua request `/v1` wajib menyertakan Authorization Bearer yang valid.
  * Fitur 1-Click Copy Key, Toggle Active/Disable, dan per-key usage analytics.
* 📊 **Dashboard Super Informatif**:
  * **In-Flight Concurrency Meter**: Menghitung request yang sedang berjalan live.
  * **Token Accounting**: Total tokens, prompt tokens, completion tokens tersimpan permanen di SQLite.
  * **Live Activity Feed**: Riwayat request live dengan latency ms, model, dan proxy yang menangani.
  * **Node Health Topology**: Grid kartu status relay dengan moving latency & success/error counters.
* 🛡️ **21 Vercel Edge Relays**: Auto-rotasi Round-Robin, Random, atau Sticky Session dengan circuit breaker failover.
* 📦 **Single Binary Standalone**:
  * Windows: `opencode-router.exe`
  * Tencent Ubuntu: `opencode-router-linux`

---

## 💻 Cara Akses Dashboard Lokal

1. Buka browser:  
   👉 **`http://127.0.0.1:8080/dashboard`**
2. Masukkan password admin:
   * Password awal: **`admin123`**
3. Di tab **🔑 API Keys Management**, buat API Key baru untuk Cursor/Cline kamu!

---

## 🐧 Deploy ke Tencent Ubuntu Server

1. Upload binary ke server:
   ```bash
   scp opencode-router-linux ubuntu@IP_SERVER_KAMU:/home/ubuntu/
   ```
2. Jalankan di Ubuntu:
   ```bash
   chmod +x opencode-router-linux
   ./opencode-router-linux
   ```
3. Buka dashboard dari browsermu:
   👉 **`http://IP_SERVER_KAMU:8080/dashboard`**
