const { connect, StringCodec } = require("nats");
const { unpack } = require("msgpackr");
const Database = require("better-sqlite3");

// 1. Initialize Local "Cache" (SQLite)
const db = new Database("local_cache.db");
const sc = StringCodec();

console.log("🚀 MicroVM starting up...");

// --- HELPER: Parse Postgres String Formats ---
function parsePostgresValue(key, value) {
  if (typeof value !== "string") return value;

  // 1. Handle Arrays (starts with '{' and ends with '}')
  // e.g. "{tag1,tag2}" or "{{1,2},{3,4}}"
  if (value.startsWith("{") && value.endsWith("}")) {
    // A quick-and-dirty parser for standard Postgres arrays
    // For production, use a library like 'postgres-array' or a proper regex state machine
    try {
      // Replace { } with [ ] to make it JSON-like
      // Note: This is a simplification. Real PG parsing is harder (quoted strings inside arrays, etc.)
      // But for your simple integers/strings it often works if formatted right.
      // A safer way for simple lists:
      const inner = value.substring(1, value.length - 1);
      if (inner.includes("{")) {
        // Matrix/Nested: "{{1,2},{3,4}}" -> "[[1,2],[3,4]]"
        const jsonStr = value.replace(/\{/g, "[").replace(/\}/g, "]");
        return JSON.parse(jsonStr);
      } else {
        // Simple Array: "tag1,tag2" -> ["tag1", "tag2"]
        // Handle quoted elements if necessary, this split is basic
        return inner.split(",").map((s) => s.replace(/"/g, ""));
      }
    } catch (e) {
      return value; // Fallback to string
    }
  }

  // 2. Handle JSONB (Postgres sends it as a stringified JSON string)
  // Your data shows: metadata => "\"{\\\"key_1\\\":...}\""
  // It might be double-encoded or just a string.
  if (
    (key === "metadata" || key.endsWith(".metadata")) &&
    value.startsWith('"')
  ) {
    try {
      return JSON.parse(JSON.parse(value)); // Double parse often needed for wire format
    } catch (e) {
      try {
        return JSON.parse(value);
      } catch (e2) {
        return value;
      }
    }
  }

  return value;
}

// --- HELPER: Ensure Table Exists ---
function ensureTable(tableName, columns) {
  // SQLite is dynamic. We treat complex types (Arrays/Objects) as TEXT (JSON stringified)
  // We filter out 'id' because we hardcode it as Primary Key
  const otherCols = Object.keys(columns)
    .filter((col) => col !== "id")
    .map((col) => `"${col}" TEXT`); // Quote column names to handle "old.age"

  const colDefs = otherCols.length > 0 ? ", " + otherCols.join(", ") : "";

  // Use "ID" logic: if data.id is missing, maybe use uid? For now assume 'id' exists.
  db.exec(
    `CREATE TABLE IF NOT EXISTS "${tableName}" (id TEXT PRIMARY KEY${colDefs})`
  );
}

async function run() {
  const nc = await connect({ servers: "nats://localhost:4222" });
  console.log("✅ Connected to NATS Hub");

  const sub = nc.subscribe("cdc.>");
  console.log("📥 Waiting for events...");

  for await (const msg of sub) {
    try {
      // 1. Decode
      const unpacked = unpack(msg.data);

      // 2. Normalize: Handle both Single Object and Array (Batch)
      const events = Array.isArray(unpacked) ? unpacked : [unpacked];

      // 3. Process the list
      for (const event of events) {
        const { table, operation, data } = event;

        if (!data || Object.keys(data).length === 0) continue;

        // --- 4. Clean Data (Filter old.* and Parse types) ---
        const cleanData = {};
        for (const [key, rawVal] of Object.entries(data)) {
          if (key.startsWith("old.")) continue; // Ignore history

          const parsed = parsePostgresValue(key, rawVal);

          if (typeof parsed === "object" && parsed !== null) {
            cleanData[key] = JSON.stringify(parsed);
          } else if (typeof parsed === "boolean") {
            cleanData[key] = parsed ? 1 : 0;
          } else {
            cleanData[key] = parsed;
          }
        }

        // --- 5. Schema & Upsert ---
        ensureTable(table, cleanData);

        const columns = Object.keys(cleanData).map((c) => `"${c}"`);
        const placeholders = columns.map(() => "?").join(", ");
        const values = Object.values(cleanData);

        // ... (Rest of your INSERT/DELETE logic remains exactly the same) ...
        if (operation === "INSERT" || operation === "UPDATE") {
          const stmt = db.prepare(
            `INSERT OR REPLACE INTO "${table}" (${columns.join(
              ", "
            )}) VALUES (${placeholders})`
          );
          stmt.run(...values);
        } else if (operation === "DELETE") {
          if (cleanData.id) {
            db.prepare(`DELETE FROM "${table}" WHERE id = ?`).run(cleanData.id);
          }
        }

        console.log(`⚡ Synced ${operation} on ${table}:${data.id || "?"}`);
      } // End of inner loop
    } catch (err) {
      console.error("❌ Error:", err);
    }
  }
}

run().catch(console.error);
