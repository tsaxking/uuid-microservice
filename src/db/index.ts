import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import { config } from 'dotenv';
config();
if (!process.env.DB_HOST) throw new Error('DB_HOST is not set');
if (!process.env.DB_PORT) throw new Error('DB_PORT is not set');
if (!process.env.DB_NAME) throw new Error('DB_NAME is not set');
if (!process.env.DB_USER) throw new Error('DB_USER is not set');
if (!process.env.DB_PASS) throw new Error('DB_PASS is not set');

export const client = postgres({
	host: process.env.DB_HOST,
	port: Number(process.env.DB_PORT),
	database: process.env.DB_NAME,
	username: process.env.DB_USER,
	password: process.env.DB_PASS
});
export const DB = drizzle(client);
