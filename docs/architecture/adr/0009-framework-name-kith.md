# ADR-0009: Framework name is "kith"

**Status:** Accepted

## Context

In an earlier monolithic, actor-centered MMO server, the underlying code was
branded with a single game's name. A generic framework branded with one game
violates bias-neutrality and creates adoption friction for other games.

## Decision

The framework is "kith" — a word for friends, neighbors, and the people
close by. The name is deliberately dual-meaning, and both readings are the
point. It evokes the interest-management core: "kith" are precisely the
actors in a player's relevance set, the ones nearby whose state streams the
player's way. And it evokes what an open-source meta-framework exists to do
— bring people together, players into communities in the worlds it hosts
and developers into a shared project. The name is applied consistently
across every project surface — the C symbol prefix, the visibility macros,
the library artifacts, the Python package, the exception hierarchy, the
metric namespace, and the runtime paths — so the identity is uniform
throughout.

## Consequences

Positive — a neutral, community-evocative name carried uniformly across every
project surface. Negative — the name is fixed from the repository's inception;
changing it later is an ABI break.
