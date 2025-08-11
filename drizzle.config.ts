import { defineConfig } from 'drizzle-kit';

if (!process.env.DB_HOST) throw new Error('DB_HOST is not set');
if (!process.env.DB_PORT) throw new Error('DB_PORT is not set');
if (!process.env.DB_NAME) throw new Error('DB_NAME is not set');
if (!process.env.DB_USER) throw new Error('DB_USER is not set');
if (!process.env.DB_PASS) throw new Error('DB_PASS is not set');

export default defineConfig({
	schema: './src/structs/*.ts',

	dbCredentials: {
		// url: process.env.DATABASE_URL,
		host: process.env.DB_HOST,
		port: Number(process.env.DB_PORT),
		database: process.env.DB_NAME,
		user: process.env.DB_USER,
		password: process.env.DB_PASS,
		ssl: false,
	},

	verbose: true,
	strict: true,
	dialect: 'postgresql',
	out: './drizzle',
	// ''
});
