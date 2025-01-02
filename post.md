Building a sharding load balancer

There has been plenty of excellent research published about sharding load
balancers.

While they haven't seen broader adoption amongst smaller technology companies, I
find them fascinating and would love to apply their benefits to various problem
spaces.

A sharding load balancer takes an incoming request and routes it to a specific
server of group of servers related to that shard. Google uses this to route
translation requests to servers with the correct model load. Facebook claims
that 46% of their services use some kind of sharded load balancer. If you have
used Cloudflare's durable objects you can think of this as a strongly consistent
sharded load balancer that consistently routes requests to a specific worker.

Let's build one.

We'll simplify the system dramatically so that it's easier for us to build and
think about. Here are the characteristics:

1. We'll use a database instead of a Raft/Paxos cluster for storing our shard
   assignments and handling consistency.
2. Instead of tracking every application, or using a dynamic pool of shards
   we'll break our system up into 1000 pre-defined shards. When an application
   is routed to a host we'll hash the id and consistently assign it to one of
   1000 shards (`hash(id) % 1000`). When we shift load around, or change the
   pool of live hosts we'll move groups of slices between hosts.

- Build a load balancer with consistent hashing.
- Add in a database, still with consistent hashing, handle updating our
  assignments
- Handle deploys and the server lifecycle.
- Start to gather stats about each slice.
- Use those stats to periodically update our slice assignments.
