import { attemptAsync } from 'ts-utils/check';
import { exec } from 'child_process';
import path from 'path';
// import * as tsNode from 'ts-node';
import url from 'url';

export const runTask = (...args: string[]) => {
	return attemptAsync(
		async () =>
			new Promise<void>((res, rej) =>
				exec(args.join(' '), (error) => {
					if (error) {
						rej(error);
					}
					res();
				})
			)
	);
};

export const runTs = async (file: string, fn: string, ...params: unknown[]) => {
	return attemptAsync(async () => {
		const fullpath = path.join(process.cwd(), file);

		const mod = await import(url.pathToFileURL(fullpath).href);
		const func = mod[fn];

		if (!func) {
			throw new Error(`Function ${fn} not found in ${file}`);
		}
		if (typeof func !== 'function') {
			throw new Error(`${fn} is not a function`);
		}

		return await func(...params);
	});
};
