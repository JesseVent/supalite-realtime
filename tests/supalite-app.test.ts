/**
 * The documented integration: attach the realtime transport to the same Node
 * server that serves a Supabase Lite app.
 *
 * This exists because the README makes that claim, and `app.server` is a *Hono*
 * app rather than a Node server — the Node server only appears once
 * `@hono/node-server`'s `serve()` wraps it. Asserting the wiring here keeps the
 * README honest, and proves the `upgrade` listener coexists with Hono's own
 * request handling instead of one swallowing the other.
 */
import type { AddressInfo } from "node:net";

import { serve } from "@hono/node-server";
import { App } from "@supabase/lite";
import { createConnection } from "@supabase/lite/sqlite";
import { RealtimeClient } from "@supabase/realtime-js";
import { afterEach, expect, it } from "vitest";

import { createRealtimeServer, type RealtimeServer } from "../src/index.js";

const API_KEY = "test-anon-key";

let cleanup: Array<() => Promise<void> | void> = [];

afterEach(async () => {
  for (const task of cleanup.reverse()) await task();
  cleanup = [];
});

it("serves realtime and HTTP on one Supalite server", async () => {
  const connection = await createConnection({ url: "file::memory:", ddlDialect: "postgres" });
  cleanup.push(() => connection.close());

  const app = new App({ connection, auth: { enabled: false } });
  await app.ensureSystemSchema();

  const server = serve({ fetch: app.server.fetch, hostname: "127.0.0.1", port: 0 });
  cleanup.push(() => new Promise<void>((resolve) => server.close(() => resolve())));
  await new Promise((resolve) => server.once("listening", resolve));
  const { port } = server.address() as AddressInfo;

  const realtime: RealtimeServer = createRealtimeServer({ server, apiKey: API_KEY });
  cleanup.push(() => realtime.close());

  // HTTP still works — the upgrade listener must not shadow normal routing.
  const health = await fetch(`http://127.0.0.1:${port}/rest/v1/`);
  expect(health.status).toBeLessThan(500);

  const client = new RealtimeClient(`ws://127.0.0.1:${port}/realtime/v1`, {
    params: { apikey: API_KEY },
    timeout: 5_000,
  });
  cleanup.push(() => client.disconnect());

  const channel = client.channel("room-1");
  const status = await new Promise<string>((resolve) => {
    channel.subscribe((value) => {
      if (value === "SUBSCRIBED" || value === "CHANNEL_ERROR" || value === "TIMED_OUT") resolve(value);
    });
  });

  expect(status).toBe("SUBSCRIBED");
  expect(realtime.subscriberCount("realtime:room-1")).toBe(1);

  const delivered = await new Promise<Record<string, unknown>>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("no broadcast")), 4_000);
    channel.on("broadcast", { event: "tick" }, (message) => {
      clearTimeout(timer);
      resolve(message);
    });
    realtime.broadcast("realtime:room-1", "tick", { ok: true });
  });

  expect(delivered["payload"]).toEqual({ ok: true });
});
