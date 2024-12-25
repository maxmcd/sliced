// @deno-types="npm:@types/dns2"
import dns2 from "npm:dns2";

// console.log(
// 	await Deno.resolveDns("google.com", "TXT", {
// 		nameServer: {
// 			ipAddr: "127.0.0.1",
// 			port: 9999,
// 		},
// 	}),
// );

export async function newDnsServer(port: number) {
	let _hosts: string[] = [];
	const server = dns2.createServer({
		udp: true,
		handle: (request, send, _rinfo) => {
			const response = dns2.Packet.createResponseFromRequest(request);
			const [question] = request.questions;
			const { name } = question;
			for (const host of _hosts) {
				response.answers.push({
					name,
					type: dns2.Packet.TYPE.TXT,
					class: dns2.Packet.CLASS.IN,
					ttl: 300,
					data: host,
				});
			}
			send(response);
		},
	});

	server.on("requestError", console.error);
	await new Promise<void>((resolve, reject) => {
		server.on("error", reject);
		server.on("listening", resolve);
		server.listen({
			udp: {
				port,
				address: "127.0.0.1",
			},
		});
	});
	return {
		server,
		updateHosts: (hosts: string[]) => {
			_hosts = hosts;
		},
	};
}
