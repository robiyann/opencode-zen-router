import os
import sqlite3
import urllib.request
import json
import time

VERCEL_TOKEN = os.environ.get("VERCEL_TOKEN", "")
DB_PATH = "router.db"

def sync_vercel():
    print("[Sync] Fetching all projects and deployments from Vercel...")
    
    # 1. Fetch deployments from Vercel
    url = "https://api.vercel.com/v6/deployments?limit=100"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {VERCEL_TOKEN}"})
    
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode())
    
    deployments = data.get("deployments", [])
    print(f"[Sync] Total deployments found on Vercel: {len(deployments)}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    new_count = 0
    total_in_db = 0

    for dep in deployments:
        dep_url = f"https://{dep['url']}"
        name = f"Relay: {dep.get('name', 'vercel-relay')}"
        state = dep.get("readyState") or dep.get("state")
        
        if state == "READY":
            # Check if exists
            cursor.execute("SELECT id FROM proxies WHERE url = ?", (dep_url,))
            row = cursor.fetchone()
            if not row:
                cursor.execute("""
                    INSERT INTO proxies (name, url, is_active, latency_ms, last_status, error_count, success_count)
                    VALUES (?, ?, 1, 0, 200, 0, 0)
                """, (name, dep_url))
                new_count += 1

    conn.commit()
    total_in_db = cursor.execute("SELECT count(*) FROM proxies").fetchone()[0]
    conn.close()

    print(f"[OK] Newly added proxies to DB : {new_count}")
    print(f"[OK] Total active proxies in DB: {total_in_db}")

if __name__ == "__main__":
    sync_vercel()
