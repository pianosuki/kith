# ADR-0011: The reactor uses io_uring on Linux

**Status:** Accepted

## Context

The framework targets "as modern as physically possible." io_uring is the
modern Linux asynchronous-I/O interface and the only event backend the
project can build, run, and test: Linux is the shipping platform, and no
supported macOS target exists. Promising a kqueue backend the project cannot
exercise would make the architecture claim more than the code does.

## Decision

The reactor library uses io_uring on Linux behind a uniform asynchronous-I/O
abstraction. Framework integrations for external services (the state and
database clients) connect through asynchronous adapters driven by reactor
readiness, never blocking calls on a reactor thread. kqueue on macOS is deferred to the 1.1.0-or-later window,
pending access to macOS hardware; nothing in the repository promises macOS
support until then. Windows support is deferred.

## Consequences

Positive — Linux is the shipping platform with best-in-class asynchronous
I/O, and the backend abstraction keeps a second event backend swappable when
macOS hardware exists. Negative — no non-Linux platform is supported until
hardware for one exists; the abstraction is an indirection the reactor pays
on every operation.
