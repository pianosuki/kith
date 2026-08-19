# minimal

The thinnest kith server: `Server(...).serve()` with no game-owned wiring.
No wire types, no handlers, no simulation model, no zone, no store, no
control routes. The composition root wires every plane with its defaults;
the gateway listener comes up on an OS-assigned ephemeral port and accepts
connections, but a peer that connects sees no decoded frames (the handler
table is empty and the replication type id is 0). This is a shipped
example, not throwaway scaffolding.

## What it proves

- The `Server` facade loads the framework shared libraries and builds the
  size-versioned creation struct.
- The C composition root wires every plane (gateway, sim, fabric, coord,
  control, db) with default tuning and reaches the CREATED state.
- The run loop drives the reactor and steps the planes until shutdown is
  requested, then drains and returns.
- The handle tears down cleanly with no game-owned wiring to release.

## Run

Python one-liner:

```
python -m examples.minimal.server
```

Prints `minimal: gateway=<port>` and blocks until `SIGINT` or `SIGTERM`,
either of which drains the server gracefully before exit. A wire client
that connects to the gateway port completes the TCP handshake but the
gateway does not decode or dispatch frames of any type.

C boot:

```
./build/debug/examples/minimal_c_boot
```

The C boot builds the handle from a NULL config (every plane wired with
its defaults), prints the bound port, installs a `SIGINT` handler that
requests graceful shutdown, and enters the run loop.

## Why no game wiring

This example is strictly thinner than `free_movement/`, which adds one
`free2d` model, one `actor_input` handler, `actor_state` as the
replication type, and four control-plane commands on top. The minimal
server is the baseline a game boot is compared against: if a game's
server fails to start, the minimal server isolates whether the failure
is in the framework wiring or in the game's own registration surface.
