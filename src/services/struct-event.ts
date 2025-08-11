import { Struct, type Blank } from 'drizzle-struct/back-end';
import { Redis } from './redis';

export const createStructEventService = (struct: Struct<Blank, string>) => {
	if (struct.data.frontend === false) return;

	struct.on('create', (data) => {
		Redis.emit(`struct:${struct.name}:create`, data);
	});

	struct.on('update', (data) => {
		Redis.emit(`struct:${struct.name}:update`, data);
	});

	struct.on('archive', (data) => {
		Redis.emit(`struct:${struct.name}:archive`, data);
	});

	struct.on('delete', (data) => {
		Redis.emit(`struct:${struct.name}:delete`, data);
	});

	struct.on('restore', (data) => {
		Redis.emit(`struct:${struct.name}:restore`, data);
	});

	struct.on('delete-version', (data) => {
		Redis.emit(`struct:${struct.name}:delete-version`, data);
	});

	struct.on('restore-version', (data) => {
		Redis.emit(`struct:${struct.name}:restore-version`, data);
	});

	struct.emit('build');
};
