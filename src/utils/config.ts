import z from 'zod';

// You may add more to the config file if you please
// Be sure to alter config.schema.json and config.example.json accordingly

export default z.object({
	environment: z.enum(['dev', 'test', 'prod']),
	app_name: z.string().min(1),
	network: z
		.object({
			host: z.string().min(1),
			port: z.number().min(1).max(65535),
			protocol: z.enum(['http', 'https'])
		})
		.optional(),
	database: z.object({
		port: z.number().min(1).max(65535),
		host: z.string().min(1),
		user: z.string().min(1),
		pass: z.string().min(1),
		name: z.string().min(1)
	}),
	redis: z.object({
		url: z.string().url(),
		name: z.string().min(1)
	}),
	logs: z.object({
		enabled: z.boolean(),
		outdir: z.string(),
		level: z.array(z.enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal', 'silent', 'struct']))
	}),
	email: z.object({
		queue_name: z.string().min(1),
		max_size: z.number().min(1),
		support_email: z.string().email()
	}),
	ntp: z.object({
		server: z.string().min(1)
	})
});
