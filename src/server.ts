/**
 * Phase 1 of ADR 0003: the Realtime transport, with no database involvement.
 *
 * Implements the two Supabase Realtime features that are pure pub/sub —
 * `broadcast` and `presence` — over the Phoenix Channels protocol that
 * `@supabase/realtime-js` speaks. `postgres_changes` needs the change-capture
 * pipeline from Phase 2 and is explicitly *refused* here rather than silently
 * accepted, because a client whose subscription is quietly ignored looks
 * identical to one whose table never changes.
 */
import { randomUUID } from "node:crypto";
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";

import { WebSocketServer, type WebSocket } from "ws";

import {
  decode,
  encode,
  isProtocolVersion,
  reply,
  VSN_2_0_0,
  type BroadcastPayload,
  type PhoenixMessage,
  type ProtocolVersion,
} from "./protocol.js";

/** Context handed to `authorize` for one connection attempt. */
export interface ConnectionContext {
  /** The `apikey` query parameter, or undefined when absent. */
  apiKey: string | undefined;
  /** Full request URL, for callers that carry their own query parameters. */
  url: URL;
  request: IncomingMessage;
}

type UpgradeListener = (request: IncomingMessage, socket: Duplex, head: Buffer) => void;

/**
 * The only thing this package needs from a server: the ability to add and
 * remove an `upgrade` listener.
 *
 * Deliberately structural rather than `http.Server`. `@hono/node-server`'s
 * `serve()` — the normal way to run a Supalite app on Node — returns
 * `ServerType`, a union of `http.Server` and `Http2Server`, which does not
 * satisfy `http.Server`. Naming the capability instead of the class means the
 * documented integration typechecks without a cast at the call site.
 */
export interface UpgradableServer {
  on(event: "upgrade", listener: UpgradeListener): unknown;
  off(event: "upgrade", listener: UpgradeListener): unknown;
}

export interface RealtimeServerOptions {
  /**
   * The server to attach to — whatever `@hono/node-server`'s `serve()` returned
   * for your Supalite app, or any `http.Server`.
   */
  server: UpgradableServer;
  /** WebSocket path. Matches the hosted Supabase route by default. */
  path?: string;
  /**
   * Expected `apikey` query parameter. Compared against the client's, which
   * `@supabase/realtime-js` always sends. Mutually exclusive with `authorize`.
   */
  apiKey?: string;
  /**
   * Custom authorisation. Return false to reject the upgrade with 401.
   * Mutually exclusive with `apiKey`.
   */
  authorize?: (context: ConnectionContext) => boolean | Promise<boolean>;
  /**
   * Drop connections that have not answered a ping within this window.
   * The client heartbeats every 25s, so the default tolerates one miss.
   */
  clientTimeoutMs?: number;
  /** Receives non-fatal protocol errors. Defaults to ignoring them. */
  onError?: (error: Error) => void;
}

interface PresenceMeta extends Record<string, unknown> {
  phx_ref: string;
}

interface Membership {
  joinRef: string | null;
  /**
   * `config.presence.key` from the join. The client sends the key *once, at
   * join time* — a later `track` carries only the user's payload — so it has
   * to be remembered here or every member is keyed by a random id.
   */
  configuredPresenceKey?: string;
  /** Set when this connection has called `track` on the topic. */
  presenceKey?: string;
  presenceMeta?: PresenceMeta;
  /** Echo this connection's own broadcasts back to it. */
  broadcastSelf: boolean;
}

interface Connection {
  socket: WebSocket;
  vsn: ProtocolVersion;
  /** Topic -> membership. A socket may join many channels. */
  topics: Map<string, Membership>;
  alive: boolean;
}

const DEFAULT_PATH = "/realtime/v1/websocket";
const DEFAULT_CLIENT_TIMEOUT_MS = 60_000;

/** A running Realtime transport. */
export interface RealtimeServer {
  /**
   * Push a broadcast to every subscriber of `topic` as if a client sent it.
   * `topic` is the bare name, without the `realtime:` prefix.
   */
  broadcast(topic: string, event: string, payload: unknown): void;
  /** Current presence state for a topic, in the shape clients receive. */
  presenceState(topic: string): Record<string, PresenceMeta[]>;
  /** Number of connections currently joined to a topic. */
  subscriberCount(topic: string): number;
  close(): Promise<void>;
}

export function createRealtimeServer(options: RealtimeServerOptions): RealtimeServer {
  if ((options.apiKey === undefined) === (options.authorize === undefined)) {
    throw new Error(
      "Provide exactly one of `apiKey` or `authorize`. A realtime endpoint with neither is open to anyone who can reach the port; pass `authorize: () => true` to say that is intended.",
    );
  }

  const path = options.path ?? DEFAULT_PATH;
  const clientTimeoutMs = options.clientTimeoutMs ?? DEFAULT_CLIENT_TIMEOUT_MS;
  const onError = options.onError ?? ((): void => {});
  const expectedApiKey = options.apiKey;
  const authorize =
    options.authorize ?? ((context: ConnectionContext): boolean => context.apiKey === expectedApiKey);

  const wss = new WebSocketServer({ noServer: true });
  const connections = new Set<Connection>();

  function send(connection: Connection, message: PhoenixMessage): void {
    if (connection.socket.readyState !== connection.socket.OPEN) return;
    try {
      connection.socket.send(encode(message, connection.vsn));
    } catch (error) {
      onError(error instanceof Error ? error : new Error(String(error)));
    }
  }

  function membersOf(topic: string): Connection[] {
    const members: Connection[] = [];
    for (const connection of connections) {
      if (connection.topics.has(topic)) members.push(connection);
    }
    return members;
  }

  function presenceStateFor(topic: string): Record<string, PresenceMeta[]> {
    const state: Record<string, PresenceMeta[]> = {};
    for (const connection of connections) {
      const membership = connection.topics.get(topic);
      if (membership?.presenceKey === undefined || membership.presenceMeta === undefined) continue;
      const existing = state[membership.presenceKey];
      if (existing === undefined) {
        state[membership.presenceKey] = [membership.presenceMeta];
      } else {
        existing.push(membership.presenceMeta);
      }
    }
    return state;
  }

  /** Wrap a flat state map in the `{metas: [...]}` envelope clients expect. */
  function withMetas(state: Record<string, PresenceMeta[]>): Record<string, { metas: PresenceMeta[] }> {
    return Object.fromEntries(Object.entries(state).map(([key, metas]) => [key, { metas }]));
  }

  function pushPresenceDiff(
    topic: string,
    joins: Record<string, PresenceMeta[]>,
    leaves: Record<string, PresenceMeta[]>,
  ): void {
    const payload = { joins: withMetas(joins), leaves: withMetas(leaves) };
    for (const member of membersOf(topic)) {
      send(member, {
        join_ref: member.topics.get(topic)?.joinRef ?? null,
        ref: null,
        topic,
        event: "presence_diff",
        payload,
      });
    }
  }

  function fanOutBroadcast(topic: string, broadcast: BroadcastPayload, from?: Connection): void {
    for (const member of membersOf(topic)) {
      const membership = member.topics.get(topic);
      if (member === from && membership?.broadcastSelf !== true) continue;
      send(member, {
        join_ref: membership?.joinRef ?? null,
        ref: null,
        topic,
        event: "broadcast",
        payload: broadcast,
      });
    }
  }

  /** Remove a topic membership and tell the channel if presence was tracked. */
  function leaveTopic(connection: Connection, topic: string): void {
    const membership = connection.topics.get(topic);
    if (membership === undefined) return;
    connection.topics.delete(topic);
    if (membership.presenceKey !== undefined && membership.presenceMeta !== undefined) {
      pushPresenceDiff(topic, {}, { [membership.presenceKey]: [membership.presenceMeta] });
    }
  }

  function handleJoin(connection: Connection, message: PhoenixMessage): void {
    const payload = (message.payload ?? {}) as {
      config?: {
        broadcast?: { self?: boolean };
        presence?: { key?: string; enabled?: boolean };
        postgres_changes?: unknown[];
      };
    };
    const config = payload.config ?? {};

    // Phase 1 has no change capture. Refusing loudly beats replying `ok` with
    // no ids, which surfaces to the user as an opaque binding mismatch.
    const requested = config.postgres_changes ?? [];
    if (requested.length > 0) {
      send(
        connection,
        reply(message, "error", {
          reason:
            "postgres_changes is not implemented by this server (ADR 0003 Phase 1 provides broadcast and presence only).",
        }),
      );
      return;
    }

    const membership: Membership = {
      joinRef: message.join_ref ?? message.ref,
      broadcastSelf: config.broadcast?.self === true,
    };
    if (config.presence?.key !== undefined && config.presence.key !== "") {
      membership.configuredPresenceKey = config.presence.key;
    }
    connection.topics.set(message.topic, membership);

    // The client matches the join reply by ref before it will report SUBSCRIBED.
    send(connection, reply(message, "ok", { postgres_changes: [] }));

    if (config.presence?.enabled === true) {
      send(connection, {
        join_ref: message.join_ref ?? message.ref,
        ref: null,
        topic: message.topic,
        event: "presence_state",
        payload: withMetas(presenceStateFor(message.topic)),
      });
    }
  }

  function handlePresence(connection: Connection, message: PhoenixMessage): void {
    const membership = connection.topics.get(message.topic);
    if (membership === undefined) {
      send(connection, reply(message, "error", { reason: "not joined" }));
      return;
    }

    const payload = (message.payload ?? {}) as {
      event?: string;
      payload?: Record<string, unknown>;
      key?: string;
    };
    const action = payload.event?.toLowerCase();

    if (action === "track") {
      const previousKey = membership.presenceKey;
      const previousMeta = membership.presenceMeta;
      const key = payload.key ?? previousKey ?? membership.configuredPresenceKey ?? randomUUID();
      const meta: PresenceMeta = { ...(payload.payload ?? {}), phx_ref: randomUUID() };
      membership.presenceKey = key;
      membership.presenceMeta = meta;

      const leaves: Record<string, PresenceMeta[]> = {};
      if (previousKey !== undefined && previousMeta !== undefined) {
        leaves[previousKey] = [previousMeta];
      }
      pushPresenceDiff(message.topic, { [key]: [meta] }, leaves);
      send(connection, reply(message, "ok", {}));
      return;
    }

    if (action === "untrack") {
      const key = membership.presenceKey;
      const meta = membership.presenceMeta;
      delete membership.presenceKey;
      delete membership.presenceMeta;
      if (key !== undefined && meta !== undefined) {
        pushPresenceDiff(message.topic, {}, { [key]: [meta] });
      }
      send(connection, reply(message, "ok", {}));
      return;
    }

    send(connection, reply(message, "error", { reason: `unknown presence event ${String(action)}` }));
  }

  function handleMessage(connection: Connection, raw: string | ArrayBuffer | Uint8Array): void {
    let message: PhoenixMessage;
    try {
      message = decode(raw, connection.vsn);
    } catch (error) {
      onError(error instanceof Error ? error : new Error(String(error)));
      return;
    }

    // The client heartbeats on the reserved `phoenix` topic and forces a
    // reconnect if the reply never lands.
    if (message.topic === "phoenix" && message.event === "heartbeat") {
      send(connection, reply(message, "ok", {}));
      return;
    }

    switch (message.event) {
      case "phx_join":
        handleJoin(connection, message);
        return;
      case "phx_leave":
        leaveTopic(connection, message.topic);
        send(connection, reply(message, "ok", {}));
        return;
      case "access_token":
        // Token rotation. Phase 1 authorises at upgrade time only; accepting
        // keeps the client's refresh loop quiet without implying enforcement.
        send(connection, reply(message, "ok", {}));
        return;
      case "broadcast": {
        if (!connection.topics.has(message.topic)) {
          send(connection, reply(message, "error", { reason: "not joined" }));
          return;
        }
        const broadcast = message.payload as BroadcastPayload;
        fanOutBroadcast(message.topic, broadcast, connection);
        send(connection, reply(message, "ok", {}));
        return;
      }
      case "presence":
        handlePresence(connection, message);
        return;
      default:
        send(connection, reply(message, "error", { reason: `unknown event ${message.event}` }));
    }
  }

  function handleUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer): void {
    let url: URL;
    try {
      url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
    } catch {
      socket.destroy();
      return;
    }
    if (url.pathname !== path) return; // Not ours; let another handler take it.

    const vsnParam = url.searchParams.get("vsn") ?? VSN_2_0_0;
    if (!isProtocolVersion(vsnParam)) {
      socket.write("HTTP/1.1 400 Bad Request\r\n\r\n");
      socket.destroy();
      return;
    }

    const context: ConnectionContext = {
      apiKey: url.searchParams.get("apikey") ?? undefined,
      url,
      request,
    };

    void Promise.resolve(authorize(context))
      .then((allowed) => {
        if (!allowed) {
          socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
          socket.destroy();
          return;
        }
        wss.handleUpgrade(request, socket, head, (ws) => {
          registerConnection(ws, vsnParam);
        });
      })
      .catch((error: unknown) => {
        onError(error instanceof Error ? error : new Error(String(error)));
        socket.destroy();
      });
  }

  function registerConnection(socket: WebSocket, vsn: ProtocolVersion): void {
    const connection: Connection = { socket, vsn, topics: new Map(), alive: true };
    connections.add(connection);

    socket.on("message", (data: Buffer | ArrayBuffer | Buffer[], isBinary: boolean) => {
      const flat = Array.isArray(data) ? Buffer.concat(data) : data;
      handleMessage(connection, isBinary ? new Uint8Array(flat as Buffer) : flat.toString());
    });
    socket.on("pong", () => {
      connection.alive = true;
    });
    socket.on("error", (error: Error) => onError(error));
    socket.on("close", () => {
      for (const topic of [...connection.topics.keys()]) leaveTopic(connection, topic);
      connections.delete(connection);
    });
  }

  // Reap sockets that stopped answering. Without this, a client that vanishes
  // without a close frame keeps its presence entry visible forever.
  const reaper = setInterval(() => {
    for (const connection of connections) {
      if (!connection.alive) {
        connection.socket.terminate();
        continue;
      }
      connection.alive = false;
      connection.socket.ping();
    }
  }, clientTimeoutMs);
  reaper.unref();

  options.server.on("upgrade", handleUpgrade);

  return {
    broadcast(topic, event, payload) {
      fanOutBroadcast(topic, { type: "broadcast", event, payload });
    },
    presenceState(topic) {
      return presenceStateFor(topic);
    },
    subscriberCount(topic) {
      return membersOf(topic).length;
    },
    async close() {
      clearInterval(reaper);
      options.server.off("upgrade", handleUpgrade);
      for (const connection of connections) connection.socket.close(1001, "server closing");
      connections.clear();
      await new Promise<void>((resolve) => wss.close(() => resolve()));
    },
  };
}
