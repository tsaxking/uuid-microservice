import { defineConfig } from 'vitest/config';
import { config } from 'dotenv';
config();
export default defineConfig({
	optimizeDeps: {
		include: ['ts-utils/**', 'drizzle-struct/**']
	},

	test: {
		include: ['src/**/*.{test,spec}.{js,ts}'],
		watch: process.argv.includes('watch')
	}
});
