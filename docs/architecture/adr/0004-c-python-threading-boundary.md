# ADR-0004: The C/Python threading boundary

**Status:** Accepted

## Context

The framework keeps its hot path in C and extends through Python. Where the
two runtimes meet decides whether Python can stall the reactor. A reactor
thread that calls into the Python interpreter acquires the global interpreter
lock and blocks every connection it normally services; without an explicit
contract, standard and free-threaded Python builds also impose differently
shaped concurrency on the rest of the runtime.

## Decision

The reactor hot path never blocks on Python work and never runs game or
handler logic inside the interpreter. Python-bound handlers are dispatched to
a worker pool outside the reactor: exactly one worker under a standard build,
N workers under a free-threaded build. The reactor runs on its own threads,
outside that pool (ADR-0011). The pool size is a runtime configuration
tunable. Free-threaded Python is a first-class target: the framework is
exercised under both the standard and the free-threaded interpreter builds.

The pool serves C callbacks, not only Python trampolines: a native C message
handler may register for the same pool-bound dispatch (ADR-0024), moving long
C handler work off the reactor thread under the same contract the Python
trampolines follow.

Bounded exception — the main-thread poll observer. An embedding whose run
loop occupies the interpreter's main thread has no other path to Python
signal delivery: the interpreter raises its interrupt exception and runs
signal handlers only in the main thread's eval loop, which sits inside the
blocking C call. The server exposes one synchronous poll-observer slot,
invoked by the run-loop thread once per tick at a safe point; its body
re-enters Python solely to pump pending signals and reports whether shutdown
was requested. The re-entry is bounded to signal pumping — one pump per
observer call, microseconds when no signal is pending — and the pump never
runs game logic; an exception raised during it is dropped and the graceful
drain requested. The Python facade installs the observer
only for a run entered on the main thread. A user signal handler that blocks
stalls the run loop until it returns; that stall is opt-in and
embedder-owned, exactly as handler duration is between byte-code boundaries
of any Python event loop.

A handler registration carries exactly one execution mode: Python-bound (a
Python trampoline, always pool-dispatched), pool-bound (a native C handler,
pool-dispatched per ADR-0024), or neither (a C handler that runs inline on
the reactor, the default for short C work).

Conformance rule — a pool-bound handler is never run inline on the reactor
thread as a fallback. When the worker pool's task queue is exhausted, the
submission fails with a busy error and the dropped input is counted —
message handlers and the per-tick callback (ADR-0014) each carry a drop
counter under the observability contract (ADR-0019); the handler is not
invoked on the reactor. A dropped input is honest backpressure; a reactor
stalled in Python — or behind a long C handler — is not. A pool-bound
registration with no worker pool attached takes the same path: the dispatch
is dropped and counted, never run inline, so the rule holds without a
configuration-dependent exception.

Execution order — pool-bound handlers of one session are unordered. With
more than one worker (the free-threaded rule above), two invocations for
the same session can be in flight at once and may complete in any order;
pool-bound C dispatch makes this observable on standard builds too, where
the interpreter lock once serialized Python handlers incidentally
(ADR-0024). The framework does not serialize same-session handler
execution — games that keep per-session state synchronize it themselves, as
they would across threads in any embedding. Control-route handlers
dispatch through the same pool under the same law: a route handler is
unordered against every other pool callback — per-tick handlers, message
handlers, other routes — and game state shared between callbacks is the
game's to synchronize. What the framework guarantees
underneath is a converging apply-and-diff structure: a session's committed
position advances through the window diff in a way that tolerates
interleaved same-session handlers, so a re-delivered or reordered input
converges the view rather than desynchronizing it. The integration suite
carries both halves of the contract: the rendezvous test proves dispatch
really runs concurrently, and the race-variant test demonstrates the
lost-update hazard unsynchronized handlers carry.

Interpreter exit — the framework refuses Python-bound callback entry once
the interpreter is exiting. The Python bridge arms an exit gate from an
interpreter exit hook, the phase that runs while the runtime still services
foreign threads and before it marks itself finalizing. While the gate is
set, every interpreter-hosted dispatch site — the server's per-tick task,
the gateway's message-dispatch task, the control plane's route task —
refuses the entry and releases its work record: a foreign C thread that
arrives through a callback trampoline has no graceful path in a finalizing
interpreter (the runtime parks threads it registered itself; foreign
entries hit its fatal paths). The control plane records its refusal under
the dispatch-drop metric; the process then exits on the host's terms with
no signal. The gate arms without synchronizing entries: under a standard
build the arming holds the interpreter lock, so no callback can be in
flight when it lands; under a free-threaded build a callback still in
flight at arming may observe tearing-down module state — the unclean-exit
shape is not a fidelity claim in either build. The gate's correctness rides
the host interpreter's ordering — exit hooks before the finalizing mark —
which is stable across the supported interpreter line but is
implementation-level, so a host that reorders it reopens the window.

## Consequences

Positive — Python cannot stall the reactor; reactor latency stays
independent of Python handler work; a free-threaded build scales
Python-bound handling by tuning the pool; a main-thread embedding receives
real signal semantics without surrendering the run-loop thread; an
embedding that dies without teardown exits cleanly instead of crashing
inside the interpreter's finalization. Negative —
every Python-bound handler crosses a thread boundary and must be thread-safe
by contract; the pool is a tunable, not a hidden constant; the poll
observer's re-entry is a narrow hole in the no-Python-on-the-run-thread
property, bounded to signal pumping and disabled unless the embedding runs
on the main thread; the exit gate is armed on a host-interpreter ordering
that is not a documented API contract.
