export const stripJsonComments = (json: string) => {
	let inString = false;
	let inSingleLineComment = false;
	let inMultiLineComment = false;
	let result = '';

	for (let i = 0; i < json.length; i++) {
		const current = json[i];
		const next = json[i + 1];

		if (inSingleLineComment) {
			if (current === '\n') {
				inSingleLineComment = false;
				result += current;
			}
			continue;
		}

		if (inMultiLineComment) {
			if (current === '*' && next === '/') {
				inMultiLineComment = false;
				i++;
			}
			continue;
		}

		if (inString) {
			if (current === '"' && json[i - 1] !== '\\') {
				inString = false;
			}
			result += current;
			continue;
		}

		if (current === '"' && !inString) {
			inString = true;
			result += current;
			continue;
		}

		if (current === '/' && next === '/') {
			inSingleLineComment = true;
			i++;
			continue;
		}

		if (current === '/' && next === '*') {
			inMultiLineComment = true;
			i++;
			continue;
		}

		result += current;
	}

	return result;
};
