# Operations

Operator-facing guidance for a running kith server: what to check at
first boot, what each recurring failure class means, and the recovery
recipe per plane. The contracts cited here are stated in the public
headers and the other guides; this page is the operational reading of
them.

## First-boot delivery

A server that decodes frames, runs handlers, and steps the sim can still
deliver nothing: replication requires the three-step session-to-delivery
contract (bind the session to an actor, seed its subscription window,
bump the changed cells into the fabric stream — the getting-started
guide's "From input to delivery"; `Session.populate` performs the first
two as one atomic step). Before load-testing delivery, verify it with a
real bound subscriber: connect a wire client, populate it, and confirm a
replication record decodes. A bare connect receives nothing by design —
an unbound session composes no view set — so a bare connect is not a
delivery probe. The silent-dead-server signature is exactly this: no
error, no metric, zero S2C bytes; walk the three steps in order.

## Window capacity and crossing churn

The cells a server tracks live in two fixed-size tables: the shared
cache (`cache_bucket_count`, 4096 cells by default, 256 per stripe at
the default split) and the gateway's fabric subscription (a fixed
interest set of 4096 cells with no configuration knob). The tracked-cell
total is the union of all sessions' windows. Size cell geometry and
neighborhood radius so the peak union fits the tables.

A `window_add` failure (`KITH_ENOMEM`) means the cell's cache stripe or
the fabric interest set has no free slot. Under sustained cell-crossing
churn the failure recurs while both gauges sit far below their caps:
the cache fills per stripe and the gauges report totals, not per-stripe
fill, so a rising ENOMEM rate is a read-the-gauges-first signal, not by
itself a saturation verdict. `Gateway.cache_stats()` (cache side) and
the subscribed-cell count (fabric side) are the gauges; when they reach
their caps, widen `cache_bucket_count` or the cell geometry — the fabric
interest set has no knob, so cell geometry is the lever there.

Failed adds roll back clean and are retained: the gateway's tick pass
retries them (bounded work per pass) until a slot frees, so a one-shot
seed heals without a caller-side retry loop. `window_remove` and
`window_clear` cancel a retained add, so a rescinded intent never
lands. The metrics surface reads the heal directly:
`kith_gateway_window_add_failures_total` counts capacity-failed adds,
`kith_gateway_window_retry_adds_total` counts the pass's landings, and
the `kith_gateway_window_retries_pending` and
`kith_gateway_sessions_without_cells` gauges report the retry queue's
depth and the count of bound sessions tracking no cells. Failures
climbing with landings landing and the queue draining is a flood that
is healing; failures with no landings and a pegged queue is capacity
that never frees — widen `cache_bucket_count` or the cell geometry.
Watch `kith_gateway_view_locate_failures_total` too: a session whose
bound actor's cell is untracked composes no view set, and that counter
is where the symptom shows.

Cell-scoped broadcasts ride the same tick pass and carry their own
pair. `kith_gateway_broadcast_refused_total` counts submits the
broadcast queue refused at its fixed depth — the submit side's
saturation signal: the game is offering more per tick than one pass
drains, so pace the traffic or treat the refusal as the line lost.
`kith_gateway_broadcast_dropped_total` counts what a drain could not
deliver: a recipient whose connection queue is full or whose session
is gone, an encode failure, a request still queued at shutdown. A
broadcast whose cell no session covers delivers to nobody and counts
nothing — an empty recipient set is not a drop.

## View capacity and freshness

The view capacity bounds what one subscriber's composed set holds: the
subscribing subject plus the closest candidates, 512 by default.
Candidates past the budget drop from the delivered individual set, and
deep enough overflow hands the overage to the crowd aggregate. A
`kith_gateway_view_candidate_high_watermark` pegged at the capacity while
`kith_gateway_view_selected_high_watermark` sits just under it is the
outgrown-budget reading — raise the capacity through the facade knob
instead of reading the missing subjects as a defect. The two refresh
intervals set the freshness floor; both derive from the tick interval, so
a wider interval is an explicit choice and its consequence is the gap it
opens — inputs published inside the gap land on the next refresh.

## Control plane

The control plane serves unauthenticated HTTP on loopback. Two response
codes carry operational meaning beyond 200:

- **500** from a route whose body exceeded the connection's write
  buffer (`write_buffer_cap`, 262144 bytes by default; the embedded
  server facade takes the same knob as
  `control_write_buffer_cap`). The condition is a capacity boundary,
  not a transient: the response is the canonical counted rejection —
  a 500 whose JSON body names the route, the attempted size, and the
  capacity (`{"error":"response_too_large",...}`) — and the
  `kith_control_response_overflow_total` series counts it. Size the
  cap for the largest listing a route returns, or paginate the route
  (`examples/_common/query_state.py` is the shipped pattern); either
  way the 500 disappears only by sizing or bounding, never by
  retrying. The built-in `/metrics` route answers the same condition
  with the same body.
- **503** when a Python-bound route cannot dispatch — the route worker
  pool is absent or its queue is full under load. Back off and retry
  with a bounded delay; a driver that treats 503 as fatal turns a load
  spike into a client stampede. Control-plane refusals under load are
  expected behavior, not server failures.

## Database operations

`kith_db_exec` failures read by class:

- `-KITH_EBUSY` through the completion callback: the pool is at its
  ceiling and every connection is busy. Backoff on this path is the
  caller's job.
- `-KITH_ECONNRESET`: the backend connection was lost mid-exchange. The
  command did not complete; re-issue it. The pool's slot goes idle and
  the next execute re-establishes the connection lazily.
- `-KITH_EBUSY` from the call itself: the reactor's task queue has no
  free node — a local backpressure signal, not a database condition.

A pool that persists mid-run runs out of band: the pool borrows a
standalone reactor, and a pool-only reactor idles out once the libpq
sockets deregister — keep the recurring keepalive timer the postgres
example wires, or every reply callback strands (the `kith.db` module's
`Database` carries that composition itself). Parameter values are
NUL-terminated text; binary payloads hex-encode.

Session statements (the `kith.db` transaction path) read by class:

- an in-flight statement's submit answers `-KITH_EBUSY` through the reply:
  a session runs one statement at a time; await each before the next.
- `-KITH_ECONNRESET` (or the collect-time `-KITH_EIO`): the session's
  connection was lost. The bracket the session carried is rolled back
  server-side by the loss itself; release the session and re-issue the
  action once the pool's reconnection has settled. A statement can never
  land on a reconnected slot — the session refuses it instead.

When the database dies, the server survives (handler containment) but
every exchange in flight fails; re-issued commands reconnect lazily once
the database returns. A game wrapper that holds one connection for the
process lifetime does not self-heal — the lazy-reconnect contract lives
in the db pool, not in a hand-held connection. For durability, commit
before acknowledging: the transaction context's COMMIT resolves before
the block exits, so code after the block — an ack built on the facade
send's live-reference contract — observes the committed state. An ack
the server sends before the commit returns is an ack the store does not
owe.

## Session death and identity

Session death is an event. The gateway fires the destroyed-session callback
when it destroys a session (a client disconnect, a full connection, or the
composition root's own destroy), delivering the session's identity record —
register it with `Server.on_session_destroyed` on the facade or
`kith_gateway_register_session_destroyed_handler` in C. The callback runs
on the worker pool; game state it touches is synchronized by the game. A
notification with no pool attached, or refused by a saturated pool, is
dropped and counted — `kith_gateway_lifecycle_dropped_total` in the metrics
export — and a growing count means per-session state is leaking: a dropped
disconnect notice is the operator's signal that the game's cleanup for that
session never ran. The live-session count reads from any thread
(`Server.session_count` / `kith_gateway_session_count`); a C composition
root snapshots live sessions on the reactor thread
(`kith_gateway_session_snapshot`).

Reconnection identity is a game-level pattern: the client re-authenticates
by principal after a reconnect and the server rebinds the same actor —
the join exchange carries the principal, the server answers with the
actor id, and replication resumes from the re-seeded window.

## Population and sweep ordering

The framework provides no idle sweep: sessions die only on close paths,
and the destroyed-session callback above is the death event. A game that
retires idle or empty groups — match instances, lobbies, rooms — owns
the ordering hazard: a group created before its first member is
populated can be observed empty and retired before population lands.
Order the game's own observations with its own population (create the
group and its first member under one game lock), or grace-period young
groups. On the session side the populate step is atomic — a session is
either unpopulated or populated — so the seed-failure census
(`kith_gateway_sessions_without_cells`) names exactly what it counts:
bound sessions whose seed failed and is awaiting the tick pass's retry,
not a populate in progress.

## Handler hygiene

Exceptions raised inside a Python handler are caught at the dispatch
trampoline: the tick keeps running, the dispatch continues, and the
exception never reaches C. Every raise is counted — the server's tick
pass records the interpreter-wide delta as the
`kith_python_handler_exceptions_total` counter, and the C getter
`kith_python_handler_exceptions` reads the process total. The first
exception per handler registration prints its traceback to stderr under
a header naming the seam; subsequent exceptions count silently. The
counter is the rate signal and the sample traceback is the diagnosis, so
handlers still log their own failures for anything that matters
operationally. `BaseException` (`KeyboardInterrupt`, `SystemExit`) is
outside the policy: it escapes to ctypes' unraisable print. An oversize
control response is outside it too: the response builders raise
`KithResponseOverflowError`, the route trampoline answers it with the
canonical counted rejection (Control plane below), and it never enters the
handler-exception counter. The guards
cover the execution the framework contracts out — the tick, message,
session-destroyed, and control-route seams; the framework's own internal
callbacks keep ctypes' unraisable print. The handler kind is the thread
boundary: the unflagged kind runs inline on the reactor thread, and the
Python facade registers the pool-dispatched kind only, so a Python
handler never runs on the reactor thread. The discrete send
(`session.send` and its facade mirrors) is contract-legal from any
context holding a live session reference — the dispatch view inside a
handler, a pinned session (`Session.pin`), or the reactor thread.

Control routes dispatch through the same worker pool as tick and message
handlers. With more than one worker the callbacks run concurrently:
game state a route handler reads alongside handler mutations needs the
game's own synchronization — a route that iterates a table a handler
mutates races, and its failure surfaces as a control-plane 500.

## Persistence composition

The shipped `GameStateStore` is an async byte-key / byte-value surface;
the transactional relational surface is the `kith.db` module —
`Database` owns the pool (reactor and keepalive included), `query`
returns rows, and `transaction()` commits before its block exits. A
game that still prefers its own client library runs it out of band over
the public seam and commits before it acknowledges. The persistence
wiring belongs in the composition root, and the DSN pins one explicit
database instance: name the TCP host and port. A provisioning step that
talks to the instance over the unix socket while the game's clients
connect over TCP can initialize one instance while the server talks to
another — they look like one database from the inside and are not.

## Persistence drills

The restart cycle — save, kill the server, reboot, load — restores
exactly what the store committed; the shipped postgres example runs it
end to end. The write-back boundary is the commit boundary: rows the
server committed before the kill survive; writes still in flight do
not. The same boundary holds against database death: acknowledged
commits survive an unclean database kill, and recovery is a server
restart once the database is back (or lazy reconnection on the next
execute, per the pool's contract above).
