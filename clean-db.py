import sqlite3

conn = sqlite3.connect("router.db")
c = conn.cursor()
c.execute("DELETE FROM proxies WHERE url LIKE '%vercel-relay-f8h3ntxdq%' OR url LIKE '%azmj6cesg%'")
deleted_count = c.rowcount
conn.commit()
conn.close()

print(f"Cleaned {deleted_count} bad proxy entries from router.db!")
