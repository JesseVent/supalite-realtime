/**
 * The Phoenix Channels wire protocol exactly as `@supabase/realtime-js` 2.97.0
 * speaks it. Verified against that package's `lib/serializer.js`.
 *
 * Two versions are on the wire:
 *
 * - `1.0.0` — every frame is a JSON *object* `{join_ref, ref, topic, event,
 *   payload}`.
 * - `2.0.0` — the client default. Frames are JSON *arrays*
 *   `[join_ref, ref, topic, event, payload]`, except broadcasts, which the
 *   client sends as a **binary** frame (kind 3) and expects back either as a
 *   JSON array or as a binary frame (kind 4).
 *
 * Getting the array-vs-object distinction wrong is the single easiest way to
 * make a client that silently never receives anything, so both are implemented
 * rather than assumed.
 */

export const VSN_1_0_0 = "1.0.0";
export const VSN_2_0_0 = "2.0.0";

export type ProtocolVersion = typeof VSN_1_0_0 | typeof VSN_2_0_0;

export function isProtocolVersion(value: string): value is ProtocolVersion {
  return value === VSN_1_0_0 || value === VSN_2_0_0;
}

/** A decoded frame. `payload` is whatever the peer put there. */
export interface PhoenixMessage {
  join_ref: string | null;
  ref: string | null;
  topic: string;
  event: string;
  payload: unknown;
}

/**
 * A broadcast payload as both sides model it: `event` is the user's event name,
 * `payload` is the user's data. When the data arrived in a binary frame it is
 * kept as an `ArrayBuffer` so it can be re-emitted without a lossy JSON trip.
 */
export interface BroadcastPayload {
  type: "broadcast";
  event: string;
  payload: unknown;
  meta?: Record<string, unknown>;
}

// Binary framing constants, mirrored from the client's serializer.
const KIND_USER_BROADCAST_PUSH = 3; // client -> server
const KIND_USER_BROADCAST = 4; // server -> client
const ENCODING_BINARY = 0;
const ENCODING_JSON = 1;
const HEADER_LENGTH = 1;
const UINT8_MAX = 255;

function isArrayBufferLike(value: unknown): value is ArrayBuffer {
  return value instanceof ArrayBuffer;
}

function toArrayBuffer(data: ArrayBuffer | Uint8Array): ArrayBuffer {
  if (data instanceof ArrayBuffer) return data;
  // Copy rather than share: a Buffer from `ws` is a view into a pooled
  // allocation, so handing out its whole backing store would leak unrelated
  // socket data into the payload.
  return data.slice().buffer as ArrayBuffer;
}

/** Decode one frame. Throws on anything malformed; callers close the socket. */
export function decode(
  raw: string | ArrayBuffer | Uint8Array,
  vsn: ProtocolVersion,
): PhoenixMessage {
  if (typeof raw !== "string") {
    return decodeBinary(toArrayBuffer(raw));
  }

  const parsed: unknown = JSON.parse(raw);

  if (vsn === VSN_1_0_0) {
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("vsn 1.0.0 frames must be JSON objects");
    }
    const frame = parsed as Partial<PhoenixMessage>;
    if (typeof frame.topic !== "string" || typeof frame.event !== "string") {
      throw new Error("frame is missing topic or event");
    }
    return {
      join_ref: frame.join_ref ?? null,
      ref: frame.ref ?? null,
      topic: frame.topic,
      event: frame.event,
      payload: frame.payload ?? {},
    };
  }

  if (!Array.isArray(parsed)) {
    throw new Error("vsn 2.0.0 frames must be JSON arrays");
  }
  const [join_ref, ref, topic, event, payload] = parsed as unknown[];
  if (typeof topic !== "string" || typeof event !== "string") {
    throw new Error("frame is missing topic or event");
  }
  return {
    join_ref: typeof join_ref === "string" ? join_ref : null,
    ref: typeof ref === "string" ? ref : null,
    topic,
    event,
    payload: payload ?? {},
  };
}

/**
 * Decode a binary broadcast push (kind 3). Layout, all lengths uint8:
 *
 *   [kind][joinRefLen][refLen][topicLen][eventLen][metaLen][encoding]
 *   [joinRef][ref][topic][event][meta][payload...]
 */
function decodeBinary(buffer: ArrayBuffer): PhoenixMessage {
  const view = new DataView(buffer);
  if (buffer.byteLength < HEADER_LENGTH + 6) {
    throw new Error("binary frame is shorter than its header");
  }
  const kind = view.getUint8(0);
  if (kind !== KIND_USER_BROADCAST_PUSH) {
    throw new Error(`unsupported binary frame kind ${kind}`);
  }

  const joinRefLength = view.getUint8(1);
  const refLength = view.getUint8(2);
  const topicLength = view.getUint8(3);
  const eventLength = view.getUint8(4);
  const metaLength = view.getUint8(5);
  const encoding = view.getUint8(6);

  const decoder = new TextDecoder();
  let offset = HEADER_LENGTH + 6;
  const read = (length: number): string => {
    const value = decoder.decode(buffer.slice(offset, offset + length));
    offset += length;
    return value;
  };

  const joinRef = read(joinRefLength);
  const ref = read(refLength);
  const topic = read(topicLength);
  const event = read(eventLength);
  const meta = read(metaLength);

  const rest = buffer.slice(offset);
  const payload: unknown =
    encoding === ENCODING_JSON ? JSON.parse(decoder.decode(rest) || "{}") : rest;

  const broadcast: BroadcastPayload = { type: "broadcast", event, payload };
  if (metaLength > 0) {
    broadcast.meta = JSON.parse(meta) as Record<string, unknown>;
  }

  return {
    join_ref: joinRef.length > 0 ? joinRef : null,
    ref: ref.length > 0 ? ref : null,
    topic,
    event: "broadcast",
    payload: broadcast,
  };
}

/**
 * Encode a frame for the client. JSON is used for everything except a
 * broadcast whose user payload is binary — sending that as JSON would corrupt
 * it, so it goes out as a kind-4 frame the client's `_decodeUserBroadcast`
 * understands.
 */
export function encode(message: PhoenixMessage, vsn: ProtocolVersion): string | Uint8Array {
  if (vsn === VSN_2_0_0 && message.event === "broadcast") {
    const payload = message.payload as BroadcastPayload | undefined;
    if (payload !== undefined && isArrayBufferLike(payload.payload)) {
      return encodeBinaryBroadcast(message.topic, payload);
    }
  }

  if (vsn === VSN_1_0_0) {
    return JSON.stringify(message);
  }
  return JSON.stringify([
    message.join_ref,
    message.ref,
    message.topic,
    message.event,
    message.payload,
  ]);
}

/**
 * Encode kind 4. Layout differs from kind 3: no join_ref/ref, since a server
 * broadcast is not a reply to anything.
 *
 *   [kind][topicLen][eventLen][metaLen][encoding][topic][event][meta][payload]
 */
function encodeBinaryBroadcast(topic: string, broadcast: BroadcastPayload): Uint8Array {
  const meta = broadcast.meta === undefined ? "" : JSON.stringify(broadcast.meta);
  const parts = { topic, event: broadcast.event, meta };
  for (const [name, value] of Object.entries(parts)) {
    if (value.length > UINT8_MAX) {
      throw new Error(`${name} length ${value.length} exceeds ${UINT8_MAX}`);
    }
  }

  const encoder = new TextEncoder();
  const topicBytes = encoder.encode(topic);
  const eventBytes = encoder.encode(broadcast.event);
  const metaBytes = encoder.encode(meta);
  const payloadBytes = new Uint8Array(broadcast.payload as ArrayBuffer);

  const header = new Uint8Array(HEADER_LENGTH + 4 + topicBytes.length + eventBytes.length + metaBytes.length);
  header[0] = KIND_USER_BROADCAST;
  header[1] = topicBytes.length;
  header[2] = eventBytes.length;
  header[3] = metaBytes.length;
  header[4] = ENCODING_BINARY;
  let offset = HEADER_LENGTH + 4;
  header.set(topicBytes, offset);
  offset += topicBytes.length;
  header.set(eventBytes, offset);
  offset += eventBytes.length;
  header.set(metaBytes, offset);

  const frame = new Uint8Array(header.length + payloadBytes.length);
  frame.set(header, 0);
  frame.set(payloadBytes, header.length);
  return frame;
}

/** Build the `phx_reply` the client's `Push` matches by `ref`. */
export function reply(
  message: Pick<PhoenixMessage, "topic" | "ref" | "join_ref">,
  status: "ok" | "error",
  response: unknown,
): PhoenixMessage {
  return {
    join_ref: message.join_ref,
    ref: message.ref,
    topic: message.topic,
    event: "phx_reply",
    payload: { status, response },
  };
}
