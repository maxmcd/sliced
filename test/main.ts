import { newDnsServer } from "./dns_server.ts";

const dnsServer = await newDnsServer(9999);
dnsServer.updateHosts(["test", "foo", "bar"]);
