import { Redis } from 'redis-utils';
import { config } from '../utils/env';

export default new Redis({
	...config.redis
});
