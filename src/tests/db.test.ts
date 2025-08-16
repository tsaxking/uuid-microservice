import testSchema from './test-schema';
import { describe, it, expect } from 'vitest';
import { config } from 'dotenv';

config();

describe('Test Schema', () => {
	it('Should pass the schema test', async () => {
		expect(await testSchema()).toEqual(true);
	});
});
