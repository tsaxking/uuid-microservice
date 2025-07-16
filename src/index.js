require('dotenv').config();

const { createClient } = require('redis');
const { open } = require('sqlite');
const sqlite3 = require('sqlite3').verbose();
const z = require('zod');
const RandomOrg = require('random-org');

const requiredEnvVars = ['API_KEY', 'HASHED_API_KEY', 'REDIS_NAME'];

for (const varName of requiredEnvVars) {
    if (!process.env[varName]) {
        throw new Error(`Environment variable ${varName} is required but not set.`);
    }
}

// -----------------------------
// Config & Globals
// -----------------------------

const DB_PATH = './uuid.db';
const MAX_STORED_IDS = 10000;
const DAILY_REQUEST_LIMIT = 1000; // max per fetch call from random.org
const FETCH_INTERVAL_MS = 60 * 60 * 1000 * 24; // fetch every day

let db;
let _pub;
let _sub;
let messageId = 1;

let totalFetched = 0;
let totalServed = 0;

// -----------------------------
// Init DB
// -----------------------------

const initDb = async () => {
	db = await open({ filename: DB_PATH, driver: sqlite3.Database });
	await db.exec(`
    CREATE TABLE IF NOT EXISTS random_ids (
      id TEXT PRIMARY KEY
    );
  `);
	console.log('[SQLite] DB ready');
};

const getStoredCount = async () => {
	const row = await db.get('SELECT COUNT(*) as count FROM random_ids');
	return row.count;
};

const storeUuids = async (uuids) => {
	const insert = await db.prepare('INSERT OR IGNORE INTO random_ids (id) VALUES (?)');
	for (const id of uuids) {
		await insert.run(id);
	}
	await insert.finalize();
	totalFetched += uuids.length;
};

const fetchUuidsFromDb = async (count) => {
	const rows = await db.all('SELECT id FROM random_ids ORDER BY created_at LIMIT ?', count);
	const ids = rows.map((r) => r.id);
	if (ids.length) {
		await db.run(
			`DELETE FROM random_ids WHERE id IN (${ids.map(() => '?').join(',')})`,
			ids
		);
		totalServed += ids.length;
	}
	return ids;
};

// -----------------------------
// Redis Init
// -----------------------------

const initRedis = async () => {
	if (_pub && _sub) return;

	const url = process.env.REDIS_URL || 'redis://localhost:6379';
	_pub = createClient({ url });
	_sub = createClient({ url });

	_pub.on('error', (err) => console.error('[Redis Pub Error]', err));
	_sub.on('error', (err) => console.error('[Redis Sub Error]', err));

	await _pub.connect();
	await _sub.connect();

	console.log('[Redis] Connected');
};

// -----------------------------
// Handle Query Requests
// -----------------------------

const querySchema = z.object({
	count: z.number().min(1).max(1000)
});

const setupQueryListener = () => {
	const channel = `query:${process.env.REDIS_NAME}:reserve`;

	_sub.subscribe(channel, async (raw) => {
		try {
			const parsed = z
				.object({
					data: z.unknown(),
					requestId: z.string(),
					responseChannel: z.string(),
					date: z.string().transform((v) => new Date(v)),
					id: z.number()
				})
				.parse(JSON.parse(raw));

			const result = querySchema.safeParse(parsed.data);
			if (!result.success) {
				console.error(`[${channel}] Invalid query`, result.error);
				return;
			}

			const { count } = result.data;
			const ids = await fetchUuidsFromDb(count);

			await _pub.publish(
				parsed.responseChannel,
				JSON.stringify({
					data: ids,
					date: new Date().toISOString(),
					id: messageId++
				})
			);
			console.log(`[query:uuid:reserve] Returned ${ids.length} UUIDs`);
		} catch (err) {
			console.error('[query:uuid:reserve] Error handling message:', err);
		}
	});
};

// -----------------------------
// Fetch UUIDs from random.org repeatedly
// -----------------------------


const startUuidFetcher = async () => {
    console.log('[UUID Store] Starting UUID fetcher...');
    const random = new RandomOrg({ apiKey: process.env.API_KEY });

    const loop = async () => {
        console.log('[UUID Store] Fetching UUIDs...');
        try {
            const current = await getStoredCount();

            if (current >= MAX_STORED_IDS) {
                console.log(`[UUID Store] Cap reached (${current}/${MAX_STORED_IDS}), skipping fetch.`);
            } else {
                const fetchCount = Math.min(DAILY_REQUEST_LIMIT, MAX_STORED_IDS - current);

                const res = await random.generateUUIDs({ n: fetchCount });
                await storeUuids(res.random.data);
                console.log(`[UUID Store] Fetched and stored ${res.random.data.length} UUIDs`);
            }
        } catch (err) {
            console.error('[random.org] Failed to fetch UUIDs:', err.message);
        }

        setTimeout(loop, FETCH_INTERVAL_MS);
    };

    loop();

	// Schedule next fetch
	setTimeout(loop, FETCH_INTERVAL_MS);
};

// -----------------------------
// Periodic metrics logging
// -----------------------------

const logMetricsLoop = () => {
	console.log(`[Metrics] Total fetched: ${totalFetched}, total served: ${totalServed}`);
	setTimeout(logMetricsLoop, 10 * 60 * 1000); // every 10 minutes
};

// -----------------------------
// Main
// -----------------------------

const main = async () => {
	await initDb();
	await initRedis();
	setupQueryListener();
	startUuidFetcher();
	logMetricsLoop();
};

main().catch(console.error);
