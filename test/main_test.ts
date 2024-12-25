import { assertEquals, assertObjectMatch } from "jsr:@std/assert";
import { newDnsServer } from "./dns_server.ts";
import { getRandomPort, Server } from "./server.ts";

Deno.test(async function addTest() {
	const clients = Array.from({ length: 5 }, (_, i) =>
		Deno.serve({ port: 0 }, (_req) => {
			return Response.json({
				user: _req.headers.get("x-user"),
				id: (i + 1).toString(),
			});
		}),
	);

	const dnsPort = getRandomPort();
	const dnsServer = await newDnsServer(dnsPort);

	const updateHosts = () => {
		dnsServer.updateHosts(
			clients.map((client) => `127.0.0.1:${client.addr.port}`),
		);
	};
	updateHosts();

	const server = await Server.startServer(dnsPort);

	const expectServer = async (user: string) => {
		const response = await fetch(`http://localhost:${server.port}`, {
			headers: { "X-User": user },
		});
		assertEquals(response.status, 200);
		const body: { user: string; id: number } = await response.json();
		assertObjectMatch(body, { user });
		return body.id;
	};

	const users = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
	const ids = await Promise.all(users.map(expectServer));
	console.log(ids);

	await clients.pop()?.shutdown();
	updateHosts();
	await new Promise((resolve) => setTimeout(resolve, 1000));
	const newIds = await Promise.all(users.map(expectServer));

	// Check that ids that were consistent before removing a server remain consistent
	for (let i = 0; i < newIds.length; i++) {
		// If the old id was from 1-4, the new id should match
		if (ids[i] <= 4) {
			assertEquals(
				newIds[i],
				ids[i],
				`User ${users[i]} changed servers unexpectedly`,
			);
		}
	}

	await Promise.all([
		server.kill(),
		Promise.all(clients.map((client) => client.shutdown())),
		dnsServer.server.close(),
	]);
});
