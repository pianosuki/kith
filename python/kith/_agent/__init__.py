"""Private home of the agentic headless client engine inside the shipped
package.

The harness tooling under ``tools/agent/`` re-exports these modules, and
the packaged visual example imports them directly. The underscore prefix
keeps the engine off the public API surface: it is harness machinery, not
framework API.
"""
