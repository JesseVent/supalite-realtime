// Live demo: a real Supalite app + realtime, two real supabase-js clients.
import { serve } from "@hono/node-server";
import { App } from "@supabase/lite";
import { createConnection } from "@supabase/lite/sqlite";
import { createClient } from "@supabase/supabase-js";
import { createRealtimeServer } from "../dist/index.js";

const ANON = "demo-anon-key";
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

const connection = await createConnection({ url: "file::memory:", ddlDialect: "postgres" });
const app = new App({ connection, auth: { enabled: false } });
await app.ensureSystemSchema();

const server = serve({ fetch: app.server.fetch, hostname: "127.0.0.1", port: 0 });
await new Promise((r) => server.once("listening", r));
const port = server.address().port;
const url = `http://127.0.0.1:${port}`;

const realtime = createRealtimeServer({ server, apiKey: ANON });
console.log(`app + realtime on ${url}\n`);

// Two independent clients, exactly as a browser would create them.
const alice = createClient(url, ANON, { realtime: { params: { apikey: ANON } } });
const bob = createClient(url, ANON, { realtime: { params: { apikey: ANON } } });

const subscribe = (ch) =>
  new Promise((res) => ch.subscribe((s) => (s === "SUBSCRIBED" ? res(s) : null)));

const aliceCh = alice
  .channel("room-1", { config: { presence: { key: "alice" } } })
  .on("broadcast", { event: "msg" }, ({ payload }) => console.log(`  alice sees: ${payload.text}`))
  .on("presence", { event: "join" }, ({ key }) => console.log(`  alice sees join: ${key}`))
  .on("presence", { event: "leave" }, ({ key }) => console.log(`  alice sees leave: ${key}`));

const bobCh = bob
  .channel("room-1", { config: { presence: { key: "bob" } } })
  .on("broadcast", { event: "msg" }, ({ payload }) => console.log(`  bob sees:   ${payload.text}`));

await subscribe(aliceCh);
await subscribe(bobCh);
console.log("both subscribed\n");

console.log("-- presence --");
await aliceCh.track({ typing: false });
await bobCh.track({ typing: true });
await wait(200);
console.log("  server state:", JSON.stringify(realtime.presenceState("realtime:room-1")).slice(0, 120));
console.log("  alice's view:", Object.keys(aliceCh.presenceState()).join(", "), "\n");

console.log("-- broadcast --");
await bobCh.send({ type: "broadcast", event: "msg", payload: { text: "hey alice" } });
await aliceCh.send({ type: "broadcast", event: "msg", payload: { text: "hey bob" } });
await wait(200);

console.log("\n-- server push (e.g. from a job) --");
realtime.broadcast("realtime:room-1", "msg", { text: "[system] deploy finished" });
await wait(200);

console.log("\n-- bob disconnects without untracking --");
await bob.realtime.disconnect();
await wait(300);
console.log("  server state:", JSON.stringify(realtime.presenceState("realtime:room-1")).slice(0, 120));

console.log("\n-- postgres_changes (Phase 2, not built) --");
const status = await new Promise((res) => {
  alice
    .channel("db")
    .on("postgres_changes", { event: "*", schema: "public", table: "items" }, () => {})
    .subscribe((s, err) => (s === "SUBSCRIBED" || s === "CHANNEL_ERROR" ? res(err?.message ?? s) : null));
});
console.log(`  ${status}`);

await alice.realtime.disconnect();
await realtime.close();
await new Promise((r) => server.close(r));
await connection.close();
console.log("\ndone");
process.exit(0);
