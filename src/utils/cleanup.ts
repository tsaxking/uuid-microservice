const listenrs = new Set<() => void>();

export const onExit = (callback: () => void) => {
	listenrs.add(callback);
};

process.on('exit', () => {
	// cleanup logic if needed
	listenrs.forEach((cb) => cb());
});
