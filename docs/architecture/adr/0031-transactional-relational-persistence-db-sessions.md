# ADR-0031: Transactional relational persistence rides connection-scoped db sessions

**Status:** Accepted

## Context

The persistence plane ships a generic async query pool: the composition root
registers named parameterized statements, execution selects a pooled connection
round-robin, and replies arrive on the reactor thread as text cells. The
byte-key/value game-state store in the examples builds on that contract. Two
structural facts keep multi-statement database transactions out of reach.
Execution round-robins across the pool, so the statements of one transaction
cannot be directed at a single connection, and a bracket opened on one pooled
connection can be completed on another. The Python package also exposes no
query-execution surface at all: game code reached the pool through the generated
bindings wrapped by hand, and a game with relational state (accounts, ledgers,
inventories) routed around the shipped surface entirely to meet its durability
bar: every action's transaction committed before the action was acknowledged,
and boot rehydrated from real result rows.

## Decision

The db module gains the session: a connection acquired exclusively from the
pool. Opening is asynchronous: the open callback receives a ready connection, a
new connection within the pool ceiling, or the pool-saturation status if neither
is available. The pinned connection is excluded from round-robin selection for
its lifetime. Session execution runs raw SQL statements on the pinned
connection, one statement per call, and the module tracks no transaction state:
BEGIN, COMMIT, and ROLLBACK are statements the caller owns, exactly like any
other SQL, which keeps the module schema-agnostic. Release is synchronous and
refuses while a session operation is in flight; it returns the connection to the
pool unmodified. A per-connection generation counter binds the session to the
concrete connection: a lazily reconnected slot fails the session rather than
inheriting an open transaction bracket. Statements that bind server-side session
state (role and authorization, search path, named prepared statements, listen
registrations, temporary tables, copy streams, two-phase prepare) are outside
the session contract: the pool resets transaction state when a connection dies,
and nothing else.

The same module gains raw-SQL execution on the pool path beside the registry:
the registry stays the named path, and registering ad-hoc queries at runtime
would race the execution contract.

The Python package gains the persistence surface over this: a database handle
that owns the pool lifecycle (its own reactor thread and the recurring keepalive
the out-of-band composition needs), an async query that returns result rows as
values, and a transaction context that opens a session, issues BEGIN, and on a
clean exit resolves COMMIT before the context exits: the
commit-before-acknowledge ordering, which pairs with the facade send's
live-reference contract (ADR-0025) for acknowledged delivery. An exception rolls
back and re-raises. Facade handlers stay synchronous; async game code drives the
surface on its own event loop. The byte-key/value store stays in the examples as
the zero-dependency default; the relational surface is the opt-in durable tier.
The facade's register-query seam is untouched, and two registration surfaces
coexist deliberately: the facade's targets a server-attached pool, the new
surface's targets its own.

**Amendment (completion and cancellation ownership):** session release is
queued, with successful completion returning the connection to the pool. A busy
completion retains the session for a retry after its statement completes.
Release itself does not roll back a transaction.

Python transaction entry, statements and exit retain ownership until their
accepted native operations complete. Entry cancellation and cancellation
escaping the block resolve the transaction before release; repeated cancellation
cannot interrupt cleanup. Cancellation is deferred rather than aborting a native
statement, so a timeout is not a hard completion deadline. Clean exit commits
despite subsequent cancellation, including cancellation before commit execution
starts. Cancellation alone therefore does not prove that no commit occurred.

A failed commit attempts rollback. Failed rollback retains the exclusive
connection instead of exposing an unresolved transaction to another caller. The
original failure takes precedence over cancellation during its cleanup. Cleanup
failures accompany the original failure, and a failed release on clean exit is
reported directly. The database retains unreleased sessions until its
destruction. Contexts exit before database closure, and their event loop remains
running through cleanup.

## Consequences

Positive: multi-statement transactions with commit-before-acknowledge are
first-class on the framework's async path; result rows are typed values rather
than byte blobs; the pool-composition machinery is framework-owned, retiring the
hand-built reactor-plus-keepalive composition the examples carry; existing
callers keep the registry path, the round-robin autocommit path, the pool
ceiling, and lazy reconnect unchanged.

Negative: the raw C surface carries one documented hazard: releasing a session
with an open transaction leaves the bracket on the pooled connection for the
next user; the Python transaction context structurally never does this, and raw
C callers roll back before release. Session-lifetime statements are out of
contract. Parameters stay text; binary transport is a separate capability.
