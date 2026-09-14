# @jessevent/supalite-realtime

Supabase Realtime for [`@supabase/lite`](https://www.npmjs.com/package/@supabase/lite),
which does not ship Realtime of its own (its README lists it as "Coming soon").

Clients connect with the ordinary `@supabase/supabase-js` / `@supabase/realtime-js`
client. No forked client, no shim.

**This is Phase 1 of [ADR 0003](docs/adr/0003-realtime-change-capture.md):
`broadcast` and `presence` only.** Both are pure pub/sub and touch no table.
`postgres_changes` needs the change-capture pipeline from Phase 2 and is
**refused with an explicit error**, not silently accepted — see below.

## Usage

Attach to the Node server that already serves your app. `app.server` is a Hono
app; the Node server is what `@hono/node-server`'s `serve()` returns.

```ts
import { serve } from "@hono/node-server";
import { App } from "@supabase/lite";
import { createConnection } from "@supabase/lite/sqlite";
import { createRealtimeServer } from "@jessevent/supalite-realtime";

const connection = await createConnection({ url: "file:app.db", ddlDialect: "postgres" });
const app = new App({ connection });
await app.ensureSystemSchema();

const server = serve({ fetch: app.server.fetch, port: 54321 });

const realtime = createRealtimeServer({
  server,
  apiKey: process.env.SUPABASE_ANON_KEY,
});
```

Clients then use the normal path, which is the hosted Supabase route
(`/realtime/v1/websocket`) and therefore the default:

```ts
const supabase = createClient("http://localhost:54321", anonKey);

supabase
  .channel("room-1")
  .on("broadcast", { event: "ping" }, ({ payload }) => console.log(payload))
  .subscribe();
```

Push from the server with `realtime.broadcast(topic, event, payload)`. The topic
is the full channel topic including the `realtime:` prefix the client adds:

```ts
realtime.broadcast("realtime:room-1", "ping", { hello: "world" });
```

## Authorisation

`createRealtimeServer` **requires** exactly one of `apiKey` or `authorize`, and
throws if given neither. A realtime endpoint reachable by anyone who can reach
the port is a decision, so it has to be made out loud:

```ts
createRealtimeServer({ server, authorize: () => true }); // explicitly open
```

`authorize` receives the parsed URL and the upgrade request, so it can read a
JWT from the query string or a cookie. It runs **at upgrade time only** —
`access_token` refresh messages are acknowledged but not re-checked, which is
fine while no table data flows over the socket and must be revisited in Phase 3.

## What works

| Feature | Status |
|---|---|
| `broadcast` between clients | ✅ |
| `broadcast` from the server | ✅ |
| `broadcast` self-echo (`config.broadcast.self`) | ✅ |
| Binary broadcast payloads (`ArrayBuffer`) | ✅ |
| `presence` track / untrack / state / diff | ✅ |
| Presence cleanup on ungraceful disconnect | ✅ |
| Heartbeats | ✅ |
| Protocol `2.0.0` (client default) and `1.0.0` | ✅ |
| `postgres_changes` | ❌ refused — Phase 2 |
| Multi-node fan-out | ❌ not planned; one embedded process is the model |

## Why `postgres_changes` errors instead of no-ops

The client validates the join reply's `postgres_changes` ids against its own
bindings. Replying `ok` with no ids surfaces as `mismatch between server and
client bindings`, which reads like a bug in your code. Replying with an error
naming the missing feature is the same amount of not-working, minus the
misdirection.

## Notes for callers

**Attach presence handlers before `subscribe()`.** This is client behaviour, not
ours: `RealtimeChannel.on` triggers `unsubscribe().then(subscribe)` when a
presence binding is added to an already-joined channel, so a late handler races
its own rejoin and can miss the first diff.

**Presence keys come from the join.** `config.presence.key` is sent once, at
join time; a later `track()` carries only your payload. Members without a
configured key get a random one.

## Tests

The suite drives the real `@supabase/realtime-js` client rather than a fixture,
because the client is the specification. That also means the binary framing is
exercised for free: the client encodes broadcasts as binary frames by default.

```
pnpm install
pnpm test
```

## Demo

`examples/demo.mjs` runs a real Supabase Lite app with two `supabase-js`
clients trading broadcasts and presence events:

```
pnpm demo
```

## License

MIT
