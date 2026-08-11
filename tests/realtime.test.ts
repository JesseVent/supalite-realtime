/**
 * End-to-end against the real `@supabase/realtime-js` client.
 *
 * The client is the specification here. Hand-rolled fixtures would only prove
 * that the server agrees with my reading of the protocol; driving the actual
 * client proves it agrees with the protocol. In particular the client encodes
 * broadcasts as *binary* frames by default, so the binary paths in
 * `protocol.ts` are exercised by every broadcast test below without being
 * mocked.
 */
import { createServer, type Server } from "node:http";
import { createConnection, type AddressInfo } from "node:net";

import { RealtimeClient } from "@supabase/realtime-js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { createRealtimeServer, type RealtimeServer } from "../src/index.js";

const API_KEY = "test-anon-key";

let http: Server;
let realtime: RealtimeServer;
let endpoint: string;
const clients: RealtimeClient[] = [];

async function listen(server: Server): Promise<number> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return (server.address() as AddressInfo).port;
}

function connect(apikey: string = API_KEY, vsn?: "1.0.0" | "2.0.0"): RealtimeClient {
  const client = new RealtimeClient(endpoint, {
    params: { apikey },
    timeout: 5_000,
    heartbeatIntervalMs: 30_000,
    ...(vsn === undefined ? {} : { vsn }),
  });
  clients.push(client);
  return client;
}

/** Subscribe and resolve on the terminal state, so failures surface as values. */
function subscribe(channel: ReturnType<RealtimeClient["channel"]>): Promise<{
  status: string;
  error?: Error;
}> {
  return new Promise((resolve) => {
    channel.subscribe((status, error) => {
      if (status === "SUBSCRIBED" || status === "CHANNEL_ERROR" || status === "TIMED_OUT") {
        resolve(error === undefined ? { status } : { status, error });
      }
    });
  });
}

function nextEvent<T>(register: (handler: (value: T) => void) => void, timeoutMs = 4_000): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out waiting for event")), timeoutMs);
    register((value) => {
      clearTimeout(timer);
      resolve(value);
    });
  });
}

/**
 * Collect every occurrence of a presence event.
 *
 * Presence handlers MUST be attached before `subscribe()`. `RealtimeChannel.on`
 * silently triggers `unsubscribe().then(subscribe)` when a presence binding is
 * added to an already-joined channel, so a handler attached later races its own
 * rejoin and misses the diff it was waiting for.
 */
function collect<T>(
  channel: ReturnType<RealtimeClient["channel"]>,
  event: "join" | "leave" | "sync",
): { events: T[]; wait: (count?: number) => Promise<T[]> } {
  const events: T[] = [];
  const push = (payload: unknown): void => {
    events.push(payload as T);
  };
  // Three distinct overloads with literal event types, so they cannot be
  // collapsed into one call on a union.
  switch (event) {
    case "sync":
      channel.on("presence", { event: "sync" }, () => push({}));
      break;
    case "join":
      channel.on("presence", { event: "join" }, push);
      break;
    case "leave":
      channel.on("presence", { event: "leave" }, push);
      break;
  }
  return {
    events,
    wait: async (count = 1) => {
      const deadline = Date.now() + 4_000;
      while (events.length < count) {
        if (Date.now() > deadline) {
          throw new Error(`timed out waiting for ${count} presence '${event}' event(s), saw ${events.length}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      return events;
    },
  };
}

/**
 * Speak the upgrade handshake by hand and return the raw status line. `fetch`
 * cannot express this — undici owns the Connection/Upgrade headers — and going
 * through a WebSocket client would hide the status code behind a generic error.
 */
function rawUpgrade(path: string): Promise<string> {
  const { port } = http.address() as AddressInfo;
  return new Promise((resolve, reject) => {
    const socket = createConnection({ host: "127.0.0.1", port }, () => {
      socket.write(
        `GET ${path} HTTP/1.1\r\n` +
          `Host: 127.0.0.1:${port}\r\n` +
          "Connection: Upgrade\r\n" +
          "Upgrade: websocket\r\n" +
          "Sec-WebSocket-Version: 13\r\n" +
          "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
      );
    });
    socket.setTimeout(4_000, () => {
      socket.destroy();
      reject(new Error("upgrade handshake timed out"));
    });
    socket.once("data", (chunk: Buffer) => {
      socket.destroy();
      resolve(chunk.toString("utf8").split("\r\n")[0] ?? "");
    });
    socket.once("error", reject);
  });
}

beforeEach(async () => {
  http = createServer((_request, response) => response.end("ok"));
  const port = await listen(http);
  endpoint = `ws://127.0.0.1:${port}/realtime/v1`;
  realtime = createRealtimeServer({ server: http, apiKey: API_KEY });
});

afterEach(async () => {
  for (const client of clients.splice(0)) await client.disconnect();
  await realtime.close();
  await new Promise<void>((resolve) => http.close(() => resolve()));
});

describe("connection", () => {
  it("subscribes a channel with the real client", async () => {
    const channel = connect().channel("room-1");
    const result = await subscribe(channel);

    expect(result.status).toBe("SUBSCRIBED");
    expect(realtime.subscriberCount("realtime:room-1")).toBe(1);
  });

  it("rejects an upgrade carrying the wrong apikey", async () => {
    const channel = connect("wrong-key").channel("room-1");
    const result = await subscribe(channel);

    expect(result.status).not.toBe("SUBSCRIBED");
    expect(realtime.subscriberCount("realtime:room-1")).toBe(0);
  });

  it("refuses postgres_changes instead of silently accepting it", async () => {
    const channel = connect()
      .channel("room-1")
      .on("postgres_changes", { event: "*", schema: "public", table: "items" }, () => {});
    const result = await subscribe(channel);

    expect(result.status).toBe("CHANNEL_ERROR");
    expect(result.error?.message).toMatch(/postgres_changes is not implemented/i);
  });

  it("requires an explicit authorisation decision", () => {
    expect(() => createRealtimeServer({ server: http })).toThrow(/exactly one of/i);
    expect(() => createRealtimeServer({ server: http, apiKey: "k", authorize: () => true })).toThrow(
      /exactly one of/i,
    );
  });
});

describe("broadcast", () => {
  it("delivers a message from one client to another", async () => {
    const sender = connect().channel("room-1");
    const received: unknown[] = [];
    const receiver = connect()
      .channel("room-1")
      .on("broadcast", { event: "ping" }, (payload) => received.push(payload));

    expect((await subscribe(sender)).status).toBe("SUBSCRIBED");
    expect((await subscribe(receiver)).status).toBe("SUBSCRIBED");

    const arrival = nextEvent<unknown>((handler) => {
      receiver.on("broadcast", { event: "ping" }, handler);
    });
    await sender.send({ type: "broadcast", event: "ping", payload: { n: 1 } });

    expect(await arrival).toMatchObject({ event: "ping", payload: { n: 1 } });
  });

  it("does not echo to the sender unless self is enabled", async () => {
    const echoed: unknown[] = [];
    const sender = connect()
      .channel("room-1")
      .on("broadcast", { event: "ping" }, (payload) => echoed.push(payload));
    const witness = connect().channel("room-1");
    expect((await subscribe(sender)).status).toBe("SUBSCRIBED");
    expect((await subscribe(witness)).status).toBe("SUBSCRIBED");

    const seen = nextEvent<unknown>((handler) => {
      witness.on("broadcast", { event: "ping" }, handler);
    });
    await sender.send({ type: "broadcast", event: "ping", payload: { n: 1 } });
    await seen;

    expect(echoed).toHaveLength(0);
  });

  it("echoes to the sender when self is enabled", async () => {
    const sender = connect().channel("room-1", { config: { broadcast: { self: true } } });
    expect((await subscribe(sender)).status).toBe("SUBSCRIBED");

    const echo = nextEvent<unknown>((handler) => {
      sender.on("broadcast", { event: "ping" }, handler);
    });
    await sender.send({ type: "broadcast", event: "ping", payload: { n: 7 } });

    expect(await echo).toMatchObject({ event: "ping", payload: { n: 7 } });
  });

  it("round-trips a binary payload without corrupting it", async () => {
    const sender = connect().channel("room-1");
    const receiver = connect().channel("room-1");
    expect((await subscribe(sender)).status).toBe("SUBSCRIBED");
    expect((await subscribe(receiver)).status).toBe("SUBSCRIBED");

    const arrival = nextEvent<{ payload: unknown }>((handler) => {
      receiver.on("broadcast", { event: "blob" }, handler);
    });
    const bytes = new Uint8Array([0, 1, 2, 250, 251, 255]);
    await sender.send({ type: "broadcast", event: "blob", payload: bytes.buffer });

    const delivered = (await arrival).payload;
    expect(delivered).toBeInstanceOf(ArrayBuffer);
    expect([...new Uint8Array(delivered as ArrayBuffer)]).toEqual([...bytes]);
  });

  it("pushes a server-originated broadcast", async () => {
    const receiver = connect().channel("room-1");
    expect((await subscribe(receiver)).status).toBe("SUBSCRIBED");

    const arrival = nextEvent<unknown>((handler) => {
      receiver.on("broadcast", { event: "tick" }, handler);
    });
    realtime.broadcast("realtime:room-1", "tick", { at: "now" });

    expect(await arrival).toMatchObject({ event: "tick", payload: { at: "now" } });
  });

  it("does not leak across topics", async () => {
    const sender = connect().channel("room-1");
    const outsider: unknown[] = [];
    const other = connect()
      .channel("room-2")
      .on("broadcast", { event: "*" }, (payload) => outsider.push(payload));
    const witness = connect().channel("room-1");
    expect((await subscribe(sender)).status).toBe("SUBSCRIBED");
    expect((await subscribe(other)).status).toBe("SUBSCRIBED");
    expect((await subscribe(witness)).status).toBe("SUBSCRIBED");

    const seen = nextEvent<unknown>((handler) => {
      witness.on("broadcast", { event: "ping" }, handler);
    });
    await sender.send({ type: "broadcast", event: "ping", payload: {} });
    await seen;

    expect(outsider).toHaveLength(0);
  });
});

describe("presence", () => {
  it("keys a member by config.presence.key, which only the join carries", async () => {
    const first = connect().channel("room-1", { config: { presence: { key: "user-1" } } });
    expect((await subscribe(first)).status).toBe("SUBSCRIBED");
    await first.track({ name: "first" });

    const second = connect().channel("room-1", { config: { presence: { key: "user-2" } } });
    const synced = collect(second, "sync");
    expect((await subscribe(second)).status).toBe("SUBSCRIBED");
    await synced.wait();

    expect(second.presenceState()).toMatchObject({ "user-1": [{ name: "first" }] });
    expect(realtime.presenceState("realtime:room-1")).toMatchObject({ "user-1": [{ name: "first" }] });
  });

  it("emits a join diff when a member tracks", async () => {
    const watcher = connect().channel("room-1");
    const joins = collect<{ key: string; newPresences: unknown[] }>(watcher, "join");
    expect((await subscribe(watcher)).status).toBe("SUBSCRIBED");

    const member = connect().channel("room-1", { config: { presence: { key: "user-9" } } });
    expect((await subscribe(member)).status).toBe("SUBSCRIBED");
    await member.track({ name: "niner" });

    const [event] = await joins.wait();
    expect(event?.key).toBe("user-9");
    expect(event?.newPresences).toMatchObject([{ name: "niner" }]);
  });

  it("emits a leave diff when a member untracks", async () => {
    const watcher = connect().channel("room-1");
    const joins = collect(watcher, "join");
    const leaves = collect<{ key: string }>(watcher, "leave");
    expect((await subscribe(watcher)).status).toBe("SUBSCRIBED");

    const member = connect().channel("room-1", { config: { presence: { key: "user-9" } } });
    expect((await subscribe(member)).status).toBe("SUBSCRIBED");
    await member.track({ name: "niner" });
    await joins.wait();

    await member.untrack();
    const [event] = await leaves.wait();

    expect(event?.key).toBe("user-9");
    expect(realtime.presenceState("realtime:room-1")).toEqual({});
  });

  it("drops presence when a member disconnects without untracking", async () => {
    const watcher = connect().channel("room-1");
    const joins = collect(watcher, "join");
    const leaves = collect<{ key: string }>(watcher, "leave");
    expect((await subscribe(watcher)).status).toBe("SUBSCRIBED");

    const client = connect();
    const member = client.channel("room-1", { config: { presence: { key: "user-9" } } });
    expect((await subscribe(member)).status).toBe("SUBSCRIBED");
    await member.track({ name: "niner" });
    await joins.wait();
    expect(realtime.presenceState("realtime:room-1")).toMatchObject({ "user-9": [{ name: "niner" }] });

    await client.disconnect();
    const [event] = await leaves.wait();

    expect(event?.key).toBe("user-9");
    expect(realtime.presenceState("realtime:room-1")).toEqual({});
  });
});

describe("protocol version 1.0.0", () => {
  // The client defaults to 2.0.0, so every other test here covers only the
  // array/binary encoding. 1.0.0 is a different wire format (plain JSON
  // objects) and would otherwise ship unexercised.
  it("subscribes and broadcasts over the object encoding", async () => {
    const sender = connect(API_KEY, "1.0.0").channel("room-1");
    const receiver = connect(API_KEY, "1.0.0").channel("room-1");
    expect((await subscribe(sender)).status).toBe("SUBSCRIBED");
    expect((await subscribe(receiver)).status).toBe("SUBSCRIBED");

    const arrival = nextEvent<unknown>((handler) => {
      receiver.on("broadcast", { event: "ping" }, handler);
    });
    await sender.send({ type: "broadcast", event: "ping", payload: { n: 1 } });

    expect(await arrival).toMatchObject({ event: "ping", payload: { n: 1 } });
  });

  it("interoperates with a 2.0.0 client on the same topic", async () => {
    const legacy = connect(API_KEY, "1.0.0").channel("room-1");
    const modern = connect(API_KEY, "2.0.0").channel("room-1");
    expect((await subscribe(legacy)).status).toBe("SUBSCRIBED");
    expect((await subscribe(modern)).status).toBe("SUBSCRIBED");

    const atModern = nextEvent<unknown>((handler) => {
      modern.on("broadcast", { event: "ping" }, handler);
    });
    await legacy.send({ type: "broadcast", event: "ping", payload: { from: "legacy" } });
    expect(await atModern).toMatchObject({ payload: { from: "legacy" } });

    const atLegacy = nextEvent<unknown>((handler) => {
      legacy.on("broadcast", { event: "pong" }, handler);
    });
    await modern.send({ type: "broadcast", event: "pong", payload: { from: "modern" } });
    expect(await atLegacy).toMatchObject({ payload: { from: "modern" } });
  });

  it("rejects an unknown protocol version at the upgrade", async () => {
    expect(await rawUpgrade(`/realtime/v1/websocket?apikey=${API_KEY}&vsn=9.9.9`)).toMatch(
      /^HTTP\/1\.1 400 /,
    );
  });

  it("accepts a known protocol version at the same endpoint", async () => {
    expect(await rawUpgrade(`/realtime/v1/websocket?apikey=${API_KEY}&vsn=2.0.0`)).toMatch(
      /^HTTP\/1\.1 101 /,
    );
  });

  it("rejects a bad apikey at the upgrade", async () => {
    expect(await rawUpgrade(`/realtime/v1/websocket?apikey=nope&vsn=2.0.0`)).toMatch(
      /^HTTP\/1\.1 401 /,
    );
  });
});

describe("lifecycle", () => {
  it("answers heartbeats so the client does not force a reconnect", async () => {
    const client = connect();
    const channel = client.channel("room-1");
    expect((await subscribe(channel)).status).toBe("SUBSCRIBED");

    const status = await new Promise<string>((resolve) => {
      client.onHeartbeat((value) => {
        if (value !== "sent") resolve(value);
      });
      client.sendHeartbeat();
    });

    expect(status).toBe("ok");
  });

  it("removes a channel's membership on unsubscribe", async () => {
    const channel = connect().channel("room-1");
    expect((await subscribe(channel)).status).toBe("SUBSCRIBED");
    expect(realtime.subscriberCount("realtime:room-1")).toBe(1);

    await channel.unsubscribe();
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(realtime.subscriberCount("realtime:room-1")).toBe(0);
  });
});
