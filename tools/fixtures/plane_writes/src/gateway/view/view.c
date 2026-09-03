/*
 * Fixture for the plane data-flow checker (tools/check_runtime_planes.py).
 * A gateway/view translation unit that calls a sim-plane authoritative-state
 * mutator (kith_sim_set_actor_pos). The self-test asserts the checker flags
 * it. Lives under tools/fixtures/ so the production checker (which scans
 * src/) never sees it.
 */

/* Forward declaration so the call parses cleanly under -std=c23 (the symbol
 * does not exist in the real headers; the fixture only exercises the call
 * detection). */
int kith_sim_set_actor_pos(void *actor, int x, int y, int z);

/* A legitimate same-plane call that must NOT be flagged. */
void fixture_view_local_helper(void);

void fixture_view_calls_mutator(void *actor)
{
    kith_sim_set_actor_pos(actor, 0, 0, 0);
    fixture_view_local_helper();
}
