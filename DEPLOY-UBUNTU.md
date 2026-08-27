# 🚀 Cara Deploy OpenCode Zen Router ke Tencent Ubuntu Server

Router ini sudah dikompilasi menjadi **single binary Linux standalone**: `opencode-router-linux`.
Binary ini **tidak butuh runtime Node.js, tidak butuh Golang di server, dan tidak butuh dependensi apapun**.

---

## 1. Upload File ke Server Tencent Ubuntu

Dari komputer kamu, upload 2 file ini ke server Tencent Ubuntu (pakai SCP, FileZilla, atau MobaXterm):
1. `opencode-router-linux`
2. `proxies.txt`

Contoh perintah SCP via terminal:
```bash
scp opencode-router-linux proxies.txt ubuntu@IP_SERVER_KAMU:/home/ubuntu/
```

---

## 2. Menjalankan di Ubuntu

Masuk ke server via SSH:
```bash
ssh ubuntu@IP_SERVER_KAMU
```

Beri izin eksekusi pada binary:
```bash
chmod +x opencode-router-linux
```

Jalankan langsung (untuk tes pertama):
```bash
./opencode-router-linux
```

Kamu akan melihat output:
```
==========================================================
🚀 OpenCode Zen Go Router started on http://localhost:8080
📦 Loaded X Vercel Proxy Relays from proxies.txt
🎯 OpenAI Compatible Endpoint: http://localhost:8080/v1
==========================================================
```

---

## 3. Menjalankan 24/7 di Background (Systemd Service)

Agar router otomatis berjalan di background dan otomatis nyala jika server reboot:

Buat service file:
```bash
sudo nano /etc/systemd/system/opencode-router.service
```

Isi dengan konfigurasi berikut:
```ini
[Unit]
Description=OpenCode Zen Go Router
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu
ExecStart=/home/ubuntu/opencode-router-linux
Restart=always
RestartSec=5
Environment=PORT=8080
Environment=PROXIES_FILE=/home/ubuntu/proxies.txt

[Install]
WantedBy=multi-user.target
```

Aktifkan dan jalankan servicenya:
```bash
sudo systemctl daemon-reload
sudo systemctl enable opencode-router
sudo systemctl start opencode-router
```

Cek status servicenya:
```bash
sudo systemctl status opencode-router
```

---

## 4. Cara Menambah Proxy Baru Kapan Saja

Jika kamu mendeploy relay Vercel baru:
1. Edit file `proxies.txt` di server:
   ```bash
   nano proxies.txt
   ```
2. Tambahkan URL proxy Vercel baru (1 per baris).
3. Restart servicenya:
   ```bash
   sudo systemctl restart opencode-router
   ```

---

## 5. Menghubungkan AI Agent ke Server Tencent

Di AI Agent kamu (Cursor, Cline, Roo Code, Python SDK):
* **Base URL**: `http://IP_SERVER_TENCENT_KAMU:8080/v1`
* **API Key**: `public` (atau bebas)
* **Model**: `nemotron-3-ultra-free` (atau model OpenCode lainnya)
