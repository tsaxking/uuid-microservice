import { Redis } from './redis';
import { z } from 'zod';

class UUIDService {
	private readonly cache: string[] = [];

	constructor(public readonly maxSize: number) {}

	get() {
		const uuid = this.cache.pop();
		if (uuid) return uuid;
		this.request();
		return crypto.randomUUID();
	}

	async request() {
		const res = await Redis.query(
			process.env.UUID_SERVICE_NAME || 'uuidServiceQueue',
			'reserve',
			{
				count: this.maxSize
			},
			z.array(z.string())
		);
		if (res.isErr()) {
			console.error('Error reserving UUIDs:', res.error);
			return;
		}
		this.cache.push(...res.value);
	}
}

const uuidService = new UUIDService(10);

export const uuid = () => {
	return uuidService.get();
};
