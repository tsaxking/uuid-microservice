import { globalCols, Struct } from 'drizzle-struct/back-end';
import { DB, client } from '../db';
import { fromCamelCase, fromSnakeCase, toSnakeCase, toCamelCase } from 'ts-utils/text';
import { attemptAsync } from 'ts-utils/check';
import fs from 'fs';
import path from 'path';

export const openStructs = () =>
	attemptAsync(async () => {
		const readFile = async (file: string): Promise<Struct[]> => {
			try {
				if (file.includes('test')) return []; // Skip test struct files
				const data = await import(file);

				const structs: Struct[] = [];

				const open = (obj: Record<string, unknown>) => {
					for (const key in obj) {
						if (key.startsWith('_')) continue;
						if (obj[key] instanceof Struct) {
							structs.push(obj[key]);
						} else if (typeof obj[key] === 'object') {
							open(obj[key] as Record<string, unknown>);
						}
					}
				};

				open(data);

				return structs;
			} catch (err) {
				console.error(err);
				return [];
			}
		};

		const readdir = async (dir: string): Promise<Struct[]> => {
			const contents = await fs.promises.readdir(dir);
			if (path.basename(dir) === 'index' || path.basename(dir) === 'test-schema') {
				return []; // Skip index and test-schema directories
			}
			const res = await Promise.all(
				contents.map(async (object) => {
					if (fs.lstatSync(path.join(dir, object)).isDirectory()) {
						return readdir(path.join(dir, object));
					}
					return readFile(path.join(dir, object));
				})
			);

			return res.flat();
		};

		const res = await readdir(path.join(process.cwd(), 'src/structs'));

		return res.sort((a, b) => a.name.localeCompare(b.name)).filter((s) => !s.data.sample);
	});
export default async (build: 'true' | 'false' = 'true') => {
	if (build === 'true') {
		await openStructs();
		await Struct.buildAll(DB);
	}

	const structs = Array.from(Struct.structs.values());

	let success = true;

	for (const s of structs) {
		const tableName = s.name;

		// 1. Get actual DB schema
		const dbCols = await client`
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = ${tableName}
        `;

		const dbColMap = new Map<string, { data_type: string; is_nullable: string }>();
		for (const row of dbCols) {
			dbColMap.set(row.column_name, {
				data_type: row.data_type,
				is_nullable: row.is_nullable
			});
		}

		// 2. Get Drizzle-defined schema
		const columns = { ...s.data.structure, ...globalCols };

		for (const [name, col] of Object.entries(columns)) {
			const expectedPgType = col.constructor.name
				.replace('Pg', '')
				.replace('Builder', '')
				.toLowerCase();

			// column in program but not in DB
			const actual = dbColMap.get(toSnakeCase(fromCamelCase(name)));
			if (!actual) {
				console.warn(`[MISSING COLUMN] ${tableName}.${name} not found in DB`);
				success = false;
				continue;
			}

			// You may want to normalize here to match 'character varying' with 'varchar' etc
			const matchesType = actual.data_type === expectedPgType;
			// const matchesNull = (actual.is_nullable === 'YES') === nullable;

			if (!matchesType) {
				console.warn(`[SCHEMA MISMATCH] ${tableName}.${name}:`);
				success = false;
			}
		}

		// check if there are extra columns in DB not defined in program
		for (const [name, _actual] of dbColMap.entries()) {
			if (!(toCamelCase(fromSnakeCase(name)) in columns)) {
				console.warn(`[EXTRA COLUMN] ${tableName}.${name} exists in DB but not in program`);
				success = false;
			}
		}
	}

	if (!success) {
		console.error(
			'Schema test failed. Please check the logs for details. You may need to push your schema changes to the database.'
		);
		process.exit(1);
	}

	console.log('Schema test passed successfully!');
	return true;
};
