import { defineConfig } from 'drizzle-kit';
import { config } from './src/utils/env';

export default defineConfig({
	schema: './src/structs/*.ts',

	dbCredentials: {
		host: config.database.host,
		port: config.database.port,
		user: config.database.user,
		password: config.database.pass,
		database: config.database.name,
		ssl: false
	},

	verbose: true,
	strict: true,
	dialect: 'postgresql',
	out: './drizzle'
});
