# ADR 0003: Realtime change capture for @supabase/lite

- Status: Proposed
- Date: 2026-08-11
- Scope: `packages/realtime` (new), thin wrapper providing Supabase Realtime
  semantics on a SQLite-backed Supalite app

## Context

`@supabase/lite` 0.8.0 does not implement Realtime. `README.md` lists it as
"Coming soon"; `STATUS.md` records only that a config schema exists
(`app/src/config/realtime.ts`), and the config surface reachable from
`IAppConfig` is the `config.toml` mirror (`enabled`, `ip_version`,
`max_header_length`) with no runtime behind it. `UPGRADE.md` confirms realtime
migration is deferred.

A wrapper therefore has to answer two independent questions:

1. **Transport / protocol** — how clients subscribe. `@supabase/realtime-js`
   (already present via `@supabase/supabase-js` 2.97.0) speaks Phoenix Channels
   over a WebSocket.
2. **Change capture** — how committed row changes are observed at all. This is
   the hard half and the subject of this ADR.

The originating request was to capture changes by reading the SQLite WAL. The
sections below record what was measured before choosing.

## Facts established by measurement

Probes were run against Node v22.22.3 with the same driver Supalite uses
(`node:sqlite` `DatabaseSync`, exposed publicly as `NodeSqliteConnection.driver`).

**The `-wal` file is a reliable, cheap change *signal*, including across
processes.** A file-level `fs.watch` on `app.db-wal` fired at **0.27–0.50 ms**
after commit in-process, and fired for **every** commit made by a separate OS
process writing the same database. A directory-level watch fired **zero** times
on macOS and must not be used. Events coalesce under rapid writes (4 events
observed for 5 back-to-back commits), so the signal is edge-triggered only — it
carries no count and no content.

**`PRAGMA data_version`** increments only when *another* connection commits, so
it is a correct cross-process staleness check but a poll, not a push.

**The SQLite session extension is available but not readable from Node.**
`db.createSession()` and `session.changeset()` work (a 2-row change produced a
44-byte changeset), but the `Session` prototype is exactly
`['changeset', 'patchset', 'close', 'constructor']` — there is **no changeset
iterator**, so consuming a changeset in JS requires hand-writing a parser for
the binary format. Worse, a table without a declared `PRIMARY KEY` produced a
**0-byte changeset**: session capture silently ignores such tables.

**Trigger-based capture works exactly as needed today.** `AFTER
INSERT/UPDATE/DELETE` triggers writing `json_object(...)` of `NEW.*`/`OLD.*` to
an outbox table produced correct logical rows — operation, primary key, and full
old/new payloads — with no decoding of any kind.

## Decision

**Use the WAL as a doorbell, not as a data source. Capture logical changes with
triggers into an append-only outbox table; use `fs.watch` on `app.db-wal` as the
sub-millisecond wake-up that says "drain the outbox now".**

The read path is ordinary SQL:

```
commit ──► trigger writes _realtime_changes row  (data, transactional)
       └─► -wal file mutates ──► fs.watch fires   (signal, ~0.3ms)
                                      │
                                      ▼
                        drain outbox WHERE seq > cursor ──► fan out to subscribers
```

The outbox `seq` is the resume cursor, which makes the pipeline restart-safe and
replayable for free.

## Rejected alternatives

**Parsing WAL frames directly.** WAL frames carry **full page images, not
logical rows**. Recovering row changes requires decoding the b-tree leaf format
(cell pointer arrays, varint payload lengths, record serial types, overflow
chains), mapping pages to tables through `sqlite_schema` root pages that move on
`VACUUM`, and *diffing each page against its previous image* to tell INSERT from
UPDATE from DELETE. A b-tree rebalance rewrites many pages while changing
nothing logically, so a naive per-page diff emits large volumes of false
changes; correctness requires diffing globally by rowid across every page in the
commit. Add `WITHOUT ROWID` tables, index pages, freelist and ptrmap pages, WAL
checkpoint/reset (salt change), and concurrent append by a live writer. This is
thousands of lines of binary code whose failure mode is a *silently wrong or
missing* realtime event. It is the most expensive available option and is
rejected outright. The WAL's genuine value here — low-latency cross-process
notification — is fully captured by watching the file, at ~0.3 ms and no parser.

**SQLite session extension.** Rejected on three measured grounds: no iterator in
`node:sqlite` (a binary parser would still be needed, merely a simpler one than
WAL pages); sessions observe only writes on *their own connection*, so any
second process is invisible; and tables without a declared `PRIMARY KEY` are
dropped silently, which is a correctness landmine rather than a documented
limit. Worth revisiting only if Node exposes `sqlite3changeset_start`.

**Polling the outbox on a timer.** Correct but strictly worse than the doorbell:
either latency or wasted wake-ups. Retained as the **fallback** where file
watching is unreliable (network filesystems, some containers), selected by
config, with `PRAGMA data_version` as the cheap "did anything change" gate.

## Consequences

- Write amplification: one extra outbox insert per changed row. Bounded by
  enabling capture per-table, never globally by default.
- Triggers must be regenerated when the schema changes, so the wrapper owns a
  reconcile step and must cooperate with Supalite's **declarative** migrator.
  `_realtime_changes` and the triggers must be passed to
  `introspection.exclude_tables`, exactly as `LITESTREAM_INTERNAL_TABLES`
  already is (see ADR 0001 lineage and `packages/core/src/contracts.ts`) —
  otherwise `migrate()` plans to drop them and throws `DataLossError`.
- The outbox needs retention (trim on drain past the slowest cursor), or it
  grows without bound and inflates every Litestream replication.
- Capture is durable and replicated, so a restored replica resumes with history
  intact.

## Decision required

**RLS enforcement is not settled and must be decided before `postgres_changes`
ships.** Hosted Supabase Realtime re-checks every changed row against the
subscriber's policies. Supalite has an RLS engine (`App._rls`, `Policy` in
deparse info) but wiring per-subscriber row authorisation is a phase of its own.
Until it exists the wrapper must **refuse to broadcast any table with RLS
enabled** rather than fan rows out to every subscriber, which would be a data
leak. Opting out requires an explicit per-table acknowledgement flag.

## Plan

Phases are independently shippable and ordered by risk.

**Phase 1 — transport only (no database involvement). Built:
`packages/realtime`.** WebSocket server attached to the caller's server via the
`upgrade` event, speaking the Phoenix Channels protocol
`@supabase/realtime-js` expects: `phx_join` / `phx_reply` / `heartbeat` on the
`phoenix` topic / `access_token`. `broadcast` and `presence` only — both are
pure pub/sub and touch no table. `postgres_changes` is refused with an explicit
error rather than accepted with no ids, which the client would surface as an
opaque binding mismatch. One dependency added, `ws` (Node has a global
WebSocket *client* but no server). 22 tests drive the real client.

Three protocol details cost a debugging cycle each and are recorded so they are
not rediscovered:

- The client's **default is vsn 2.0.0**, whose frames are JSON *arrays*
  `[join_ref, ref, topic, event, payload]`, and whose broadcasts are **binary**
  frames. vsn 1.0.0's plain JSON objects are a separate encoding; both are
  implemented and tested.
- `config.presence.key` is sent **only in the join**. A later `track()` carries
  just the user payload, so the key must be remembered per membership or every
  member is keyed by a random id.
- `RealtimeChannel.on` silently triggers `unsubscribe().then(subscribe)` when a
  presence binding is added to an already-joined channel — a caller-visible
  behaviour that makes late presence handlers race their own rejoin.

**Phase 2 — change capture and `postgres_changes`.** Outbox table, trigger
generation and reconcile, `exclude_tables` wiring, the `fs.watch` doorbell with
the polling fallback, drain-and-fan-out with the `seq` cursor, retention. Filter
subscriptions by `schema`/`table`/`eq` filter. RLS-enabled tables refused per the
open decision above.

**Phase 3 — RLS-aware delivery.** Per-subscriber row authorisation, which
retires the Phase 2 refusal.

Not planned: WAL frame parsing (rejected above), and multi-node fan-out — a
single embedded process is the deployment model this repo already targets.
