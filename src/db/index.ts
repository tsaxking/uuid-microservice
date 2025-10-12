import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import { onExit } from '../utils/cleanup';
import { config } from '../utils/env';

export const client = postgres({
	host: config.database.host,
	port: config.database.port,
	database: config.database.name,
	username: config.database.user,
	password: config.database.pass
});
export const DB = drizzle(client);
onExit(() => {
	client.end();
	console.log('Database connection closed');
});
