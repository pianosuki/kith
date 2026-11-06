# ADR-0011: The reactor uses io_uring on Linux

**Status:** Accepted

## Context

The framework targets "as modern as physically possible." io_uring is the modern
Linux asynchronous-I/O interface and the only event backend the project can
build, run, and test: Linux is the shipping platform, and no supported macOS
target exists. Promising a kqueue backend the project cannot exercise would make
the architecture claim more than the code does.

## Decision

The reactor library uses io_uring on Linux behind a uniform asynchronous-I/O
abstraction. Framework integrations for external services (the state and
database clients) connect through asynchronous adapters driven by reactor
readiness, never blocking calls on a reactor thread. kqueue on macOS is deferred
to the 1.1.0-or-later window, pending access to macOS hardware; nothing in the
repository promises macOS support until then. Windows support is deferred.

**Amendment (callback stop requests):** readiness and completion callbacks may
request that their reactor stop. A stop request does not re-enter dispatch or
release handles. Service reply callbacks remain nonblocking and do not call
other reactor operations; owners observe run completion and the existing
lifetime preconditions before destroying handles.

**Amendment (registration retirement):** deleting a registered descriptor ends
readiness callback and context ownership synchronously, including when backend
cancellation cannot be admitted or submitted. An executing callback is not
interrupted; later readiness cannot use the retired context. Cancellation errors
remain observable. Pending backend work remains reactor-owned until completion
or destruction. Replacement registrations use distinct identities; exhaustion
refuses identity-changing operations rather than reusing an identity. Accepted
tasks retain their existing ownership and processing contract.

## Consequences

Positive: Linux is the shipping platform with modern asynchronous I/O, and the
backend abstraction keeps a second event backend swappable when macOS hardware
exists. Negative: no non-Linux platform is supported until hardware for one
exists; the abstraction is an indirection the reactor pays on every operation.
