# ADR-0031: Transactional relational persistence rides connection-scoped db sessions

**Status:** Accepted

## Context

The persistence plane ships a generic async query pool: the composition root
registers named parameterized statements, execution selects a pooled
connection round-robin, and replies arrive on the reactor thread as text
cells. The byte-key/value game-state store in the examples builds on that
contract. Two structural facts keep multi-statement database transactions out
of reach. Execution round-robins across the pool, so the statements of one
transaction cannot be directed at a single connection, and a bracket opened
on one pooled connection can be completed on another. And the Python package
exposes no query-execution surface at all: game code reached the pool through
the generated bindings wrapped by hand, and a game with relational state —
accounts, ledgers, inventories — routed around the shipped surface entirely
to meet its durability bar: every action's transaction committed before the
action was acknowledged, and boot rehydrated from real result rows.

## Decision

The db module gains the session: a connection acquired exclusively from the
pool. Opening is asynchronous — a ready connection, else a new connect within
the pool ceiling, else the pool-saturation status through the open callback —
and the pinned connection is excluded from round-robin selection for its
lifetime. Session execution runs raw SQL statements on the pinned connection,
one statement per call, and the module tracks no transaction state: BEGIN,
COMMIT, and ROLLBACK are statements the caller owns, exactly like any other
SQL, which keeps the module schema-agnostic. Release is synchronous and
refuses while a session operation is in flight; it returns the connection to
the pool unmodified. A per-connection generation counter binds the session to
the concrete connection: a lazily reconnected slot fails the session rather
than inheriting an open transaction bracket. Statements that bind server-side
session state — role and authorization, search path, named prepared
statements, listen registrations, temporary tables, copy streams, two-phase
prepare — are outside the session contract: the pool resets transaction
state when a connection dies, and nothing else.

The same module gains raw-SQL execution on the pool path beside the
registry: the registry stays the named path, and registering ad-hoc queries
at runtime would race the execution contract.

The Python package gains the persistence surface over this: a database
handle that owns the pool lifecycle — its own reactor thread and the
recurring keepalive the out-of-band composition needs — an async query that
returns result rows as values, and a transaction context that opens a
session, issues BEGIN, and on a clean exit resolves COMMIT before the
context exits: the commit-before-acknowledge ordering, which pairs with the
facade send's live-reference contract (ADR-0025) for acknowledged delivery. An
exception rolls back and re-raises. Facade handlers stay synchronous; async
game code drives the surface on its own event loop. The byte-key/value store
stays in the examples as the zero-dependency default; the relational surface
is the opt-in durable tier. The facade's register-query seam is untouched,
and two registration surfaces coexist deliberately: the facade's targets a
server-attached pool, the new surface's targets its own.

## Consequences

Positive — multi-statement transactions with commit-before-acknowledge are
first-class on the framework's async path; result rows are typed values
rather than byte blobs; the pool-composition machinery is framework-owned,
retiring the hand-built reactor-plus-keepalive composition the examples
carry; existing callers keep the registry path, the round-robin autocommit
path, the pool ceiling, and lazy reconnect unchanged.

Negative — the raw C surface carries one documented hazard: releasing a
session with an open transaction leaves the bracket on the pooled connection
for the next user; the Python transaction context structurally never does
this, and raw C callers roll back before release. Session-lifetime
statements are out of contract. Parameters stay text; binary transport is a
separate capability.
