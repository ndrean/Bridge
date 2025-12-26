import asyncio
import sqlite3
import msgpack
from flask import Flask, jsonify
from nats.aio.client import Client as NATS

app = Flask(__name__)
nc = NATS()

# 1. Init Local Cache (SQLite)
# In a real microVM, this file persists in the /tmp or volume
def init_db():
    conn = sqlite3.connect("cache.db")
    # Store everything as JSON/Text for simplicity in this demo
    conn.execute("CREATE TABLE IF NOT EXISTS kv_store (table_name TEXT, id TEXT, data TEXT, PRIMARY KEY (table_name, id))")
    conn.close()

init_db()

# 2. Background Worker: The "Pump"
async def nats_worker():
    await nc.connect("nats://localhost:4222")
    print("✅ Connected to NATS")
    
    # Subscribe to ALL tables
    sub = await nc.subscribe("cdc.>")
    
    async for msg in sub:
        try:
            # Zero-copy decode
            event = msgpack.unpackb(msg.data)
            table = event['table']
            op = event['operation']
            data = event['data']
            
            conn = sqlite3.connect("cache.db")
            
            if op in ('INSERT', 'UPDATE'):
                # Upsert logic
                import json
                conn.execute(
                    "INSERT OR REPLACE INTO kv_store (table_name, id, data) VALUES (?, ?, ?)",
                    (table, str(data['id']), json.dumps(data))
                )
            elif op == 'DELETE':
                conn.execute("DELETE FROM kv_store WHERE table_name = ? AND id = ?", (table, str(data['id'])))
            
            conn.commit()
            conn.close()
            print(f"⚡ Synced {op} on {table}:{data.get('id')}")
            
        except Exception as e:
            print(f"Error: {e}")

# Start NATS in background when Flask starts
@app.before_serving
async def start_nats():
    asyncio.create_task(nats_worker())

# 3. The "Zero Latency" Read Endpoint
@app.route('/users/<id>')
async def get_user(id):
    conn = sqlite3.connect("cache.db")
    cursor = conn.execute("SELECT data FROM kv_store WHERE table_name = ? AND id = ?", ('users', id))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return row[0], 200, {'Content-Type': 'application/json'}
    return jsonify({"error": "Not found (or not synced yet)"}), 404