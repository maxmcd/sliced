import { rootDir } from "./rootdir.ts";

export function getRandomPort() {
	const server = Deno.listen({ port: 0 });
	const { port } = server.addr as Deno.NetAddr;
	server.close();
	return port;
}

export class Server {
	private constructor(
		private readonly command: Deno.ChildProcess,
		private readonly _port: number,
	) {}
	static async startServer(dnsPort: number) {
		const port = await getRandomPort();
		const command = new Deno.Command("cargo", {
			args: ["run", "-p", "server", "--", port.toString(), dnsPort.toString()],
			cwd: rootDir,
			stdin: "inherit",
			stdout: "inherit",
		});
		const child = await command.spawn();

		// Wait for server to be ready by polling until we get a 200 response
		const startTime = Date.now();
		while (true) {
			try {
				const response = await fetch(`http://localhost:${port}`);
				await response.body?.cancel();
				if (response.ok) {
					break;
				}
			} catch (e) {}
			if (Date.now() - startTime > 5000) {
				throw new Error("Server failed to start within 5 seconds");
			}
			// Ignore connection errors while server is starting up
			await new Promise((resolve) => setTimeout(resolve, 100));
		}
		return new Server(child, port);
	}

	get port() {
		return this._port;
	}

	async kill() {
		this.command.kill("SIGINT");
		return await this.command.status;
	}
}
