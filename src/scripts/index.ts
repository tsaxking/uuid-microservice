import path from 'path';
import { runTs } from '../utils/task';
import fs from 'fs/promises';
import { select, prompt } from '../cli/utils';

// Convert import.meta.url to a file path

const main = async () => {
	let [, , file, ...args] = process.argv;

	if (!file) {
		const scripts = (await fs.readdir(path.join(process.cwd(), 'scripts'))).filter(
			(s) => s.endsWith('.ts') && s !== 'index.ts'
		);
		const chosen = (
			await select({
				message: 'Select a script to run',
				options: scripts.map((s) => ({
					name: s,
					value: s
				}))
			})
		).unwrap();
		if (!chosen) {
			process.exit();
		}
		file = chosen;
		args =
			(
				await prompt({
					message: 'Enter arguments (space separated)'
				})
			)
				.unwrap()
				?.split(' ') ?? [];
	}

	console.log('Running file:', file);

	const res = await runTs(path.join('scripts', file), 'default', ...args);

	if (res.isErr()) {
		console.error(res.error);
		process.exit(1);
	}

	if (res.isOk()) {
		console.log(res.value);
		process.exit(0);
	}
};

main();