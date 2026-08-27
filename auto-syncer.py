import os
import sqlite3
import urllib.request
import json
import time

VERCEL_TOKEN = os.environ.get("VERCEL_TOKEN", "")
DB_PATH = "router.db"

def sync_once():
    url = "https://api.vercel.com/v6/deployments?limit=100"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {VERCEL_TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        
        deployments = data.get("deployments", [])
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_count = 0

        for dep in deployments:
            dep_url = f"https://{dep['url']}"
            name = f"Relay: {dep.get('name', 'vercel-relay')}"
            state = dep.get("readyState") or dep.get("state")
            
            if state == "READY":
                cursor.execute("SELECT id FROM proxies WHERE url = ?", (dep_url,))
                if not cursor.fetchone():
                    cursor.execute("""
                        INSERT INTO proxies (name, url, is_active, latency_ms, last_status, error_count, success_count)
                        VALUES (?, ?, 1, 0, 200, 0, 0)
                    """, (name, dep_url))
                    new_count += 1

        conn.commit()
        total_in_db = cursor.execute("SELECT count(*) FROM proxies").fetchone()[0]
        conn.close()
        if new_count > 0:
            print(f"[Auto-Sync] +{new_count} new nodes injected! Total in DB: {total_in_db}", flush=True)
    except Exception as e:
        pass

if __name__ == "__main__":
    print("[Auto-Sync] Watcher started. Monitoring Vercel deployments...", flush=True)
    for _ in range(60): # Run for 15 minutes
        sync_once()
        time.sleep(15)
    print("[Auto-Sync] Watcher finished.", flush=True)
