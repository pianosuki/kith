"""Population view census and isolation attribution."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace

from examples._common.query_state import QUERY_STATE_DEFAULT_PAGE_SIZE
from examples.spatial.handlers import CELL_RADIUS, CELL_SIZE_Q16

from tools.agent.assertions import ServerControl
from tools.agent.orchestrator import EvidenceReader, EvidenceSnapshot, Orchestrator

from .constants import BOOTSTRAP_READY, ISOLATED_FRACTION_MAX, QUERY_STATE_ERRORS
from .depth import client_view_facts, distinct_non_self, principal_bindings, view_evidence_fresh
from .metrics import BreadthAttribution, BreadthCensus, SparseBreadthRow
from .records import ClientRecord


async def roster_positions(control: ServerControl) -> dict[int, tuple[int, int, int]]:
    """Return the server's actor→position map from paginated ``/query_state``.

    Pages the route exactly like ``/bindings`` and folds every page into
    one mapping. A transport failure or a malformed entry raises instead
    of degrading: an under-counted roster would misclassify clients as
    isolated and silently shrink the breadth denominator.

    Callers with several instances invoke this per instance control —
    each instance allocates actor ids independently from 1, so one flat
    fetch through the aggregating control would collide ids across
    instances.
    """
    positions: dict[int, tuple[int, int, int]] = {}
    offset = 0
    limit = QUERY_STATE_DEFAULT_PAGE_SIZE
    while True:
        try:
            mapping: Mapping[str, object] = await control.get(
                f"/query_state?offset={offset}&limit={limit}"
            )
        except QUERY_STATE_ERRORS as exc:
            raise RuntimeError(f"roster positions unavailable ({exc})") from exc
        actors = mapping.get("actors")
        if not isinstance(actors, list):
            raise RuntimeError("roster positions unavailable (/query_state response malformed)")
        for item in actors:
            if (
                not isinstance(item, dict)
                or "actor_id" not in item
                or "pos_x" not in item
                or "pos_y" not in item
                or "pos_z" not in item
            ):
                raise RuntimeError("/query_state roster entry malformed")
            positions[int(item["actor_id"])] = (
                int(item["pos_x"]),
                int(item["pos_y"]),
                int(item["pos_z"]),
            )
        # Stop on a short final page, or when the server does not paginate
        # (no ``total`` field — the back-compat full-roster response).
        if len(actors) < limit or "total" not in mapping:
            break
        offset += limit
    return positions


def isolated_actor_ids(
    positions: Mapping[int, tuple[int, int, int]],
    candidate_ids: Sequence[int],
    min_distinct_others: int,
    *,
    cell_size: int = CELL_SIZE_Q16,
    cell_radius: int = CELL_RADIUS,
) -> frozenset[int]:
    """Return candidates whose radius-1 cell neighborhood is too sparse.

    A candidate's own actor at window end must see at least
    ``min_distinct_others`` OTHER actors within ``cell_radius`` cells of
    its own on every axis (the same geometry the gateway publishes into)
    to have been structurally able to observe a meaningful view set;
    those that cannot are isolated and reported rather than counted
    against delivery. Buckets hold every roster actor per integer cell,
    so counting walks only 27 neighborhood keys regardless of world size.
    """
    if cell_radius < 1:
        raise ValueError("cell_radius must be >= 1")
    buckets: dict[tuple[int, int, int], list[int]] = {}
    for actor_id, (x, y, z) in positions.items():
        buckets.setdefault((x // cell_size, y // cell_size, z // cell_size), []).append(actor_id)
    axis_range = range(-cell_radius, cell_radius + 1)
    isolated: set[int] = set()
    for own_id in candidate_ids:
        own_pos = positions.get(own_id)
        if own_pos is None:
            # The binding map promised this principal owns an actor; a
            # roster missing it is unusable truth.
            raise RuntimeError(f"roster omits own actor {own_id}")
        cx, cy, cz = own_pos[0] // cell_size, own_pos[1] // cell_size, own_pos[2] // cell_size
        neighbors = 0
        for dx in axis_range:
            for dy in axis_range:
                for dz in axis_range:
                    members = buckets.get((cx + dx, cy + dy, cz + dz))
                    if members:
                        neighbors += len(members)
        # Self contributes once when present; subtract defensively rather
        # than trusting the binding invariant twice.
        members = buckets.get((cx, cy, cz))
        if members and own_id in members:
            neighbors -= 1
        if neighbors < min_distinct_others:
            isolated.add(own_id)
    return frozenset(isolated)


@dataclass(slots=True)
class BreadthRow:
    """One record's collection-time breadth state, built across passes.

    The probe pass fills session and view truth; the classification pass
    fills the binding and geometry flags; the fresh pass runs only for
    isolated rows. The assembly then reads the flags without I/O.
    """

    name: str
    instrumented: bool
    submitted: int
    connected: bool
    instance_index: int = 0
    session_ok: bool = False
    observed: bool = False
    bound: bool = False
    isolated: bool = False
    fresh: bool = False
    late_bootstrapped: bool = False
    # Cleared the bar on distinct ids but gapped past the stall bound
    # mid-window: a delivery break the distinct count alone hides.
    stalled: bool = False
    # The retained-stream basis cannot prove completeness (eviction under
    # the window, or no byte budget): the counts are a floor, so the row
    # reads unobserved and is counted separately from sparse streams.
    incomplete: bool = False
    # The counts behind the observed reading, kept for the residue table:
    # the deque set ignores the window bounds, so a sparse window read
    # splits into window-too-young versus stream-never-carried. Both
    # counts exclude the client's own actor id (read-time self filter).
    window_present: bool = False
    distinct_in_window: int = 0
    distinct_deque: int = 0


def isolation_cause(row: BreadthRow) -> str:
    """Name why an isolated row's actor sits outside the pile geometry.

    ``late_bootstrapped`` implies never-movable (the movement phase skips
    records without a stamped boot), so its actor never left the spawn
    cell; the remaining classes split on session and submit state.
    """
    if row.late_bootstrapped:
        return "late_bootstrap"
    if not row.connected:
        return "disconnected"
    if row.submitted == 0:
        return "stalled"
    return "frozen"


def residue_rows(rows: Iterable[BreadthRow]) -> tuple[SparseBreadthRow, ...]:
    """Name and count the rows the breadth classes leave unexplained.

    A row lands here when it is unobserved with neither a stall nor an
    incomplete basis behind the reading; the window presence splits it
    into ``sparse`` versus ``never_opened``. The caller narrows to the
    cohort the ratio reads (bound, non-isolated rows where a roster
    classifies, the full population otherwise).
    """
    return tuple(
        SparseBreadthRow(
            name=row.name,
            instance_index=row.instance_index,
            klass="never_opened" if not row.window_present else "sparse",
            distinct_in_window=row.distinct_in_window,
            distinct_deque=row.distinct_deque,
        )
        for row in rows
        if not row.observed and not row.stalled and not row.incomplete
    )


def assemble_breadth(
    total: int,
    rows: Sequence[BreadthRow],
    *,
    classified: bool,
    fresh_isolated_violates: bool = True,
    evidence_spans: Sequence[int] = (),
    instance_index: int | None = None,
) -> BreadthCensus:
    """Reduce the fetched breadth rows to the census (pure; no I/O).

    Containment is structural: a row counts as selected only when it is
    bound, non-isolated, and observed, so the numerator is a subset of the
    denominator cohort. Rows observed while isolated are attributed and
    rendered instead of counted; fresh evidence among them is the
    divergence the selected>eligible guardrail exists to catch, and any
    occurrence breaches the guardrail alongside the existing
    isolated-fraction bound where ``fresh_isolated_violates`` scopes the
    guard in (see :func:`_collect_breadth` for the topology scoping).
    """
    session_ok_count = sum(1 for row in rows if row.session_ok)
    if not classified:
        residue = residue_rows(rows)
        return BreadthCensus(
            total=total,
            session_ok_count=session_ok_count,
            selected_count=sum(1 for row in rows if row.observed),
            selected_head_count=sum(1 for row in rows if row.observed and row.instrumented),
            stalled_clients=sum(1 for row in rows if row.stalled),
            incomplete_evidence_clients=sum(1 for row in rows if row.incomplete),
            sparse_clients=sum(1 for entry in residue if entry.klass == "sparse"),
            never_opened_clients=sum(1 for entry in residue if entry.klass == "never_opened"),
            sparse_rows=residue,
            instance_index=instance_index,
        )
    bound_rows = [row for row in rows if row.bound]
    isolated_rows = [row for row in bound_rows if row.isolated]
    fresh_rows = [row for row in isolated_rows if row.fresh]
    selected = 0
    selected_head = 0
    for row in bound_rows:
        if row.isolated or not row.observed:
            continue
        selected += 1
        if row.instrumented:
            selected_head += 1
    classified_count = len(bound_rows)
    isolated_count = len(isolated_rows)
    violations: list[str] = []
    if classified_count and isolated_count > ISOLATED_FRACTION_MAX * classified_count:
        violations.append(
            f"{isolated_count} of {classified_count} classified clients lack their "
            f"neighborhood geometry; the window-end roster cannot support "
            f"breadth eligibility"
        )
    if fresh_rows and fresh_isolated_violates:
        names = ", ".join(row.name for row in fresh_rows)
        violations.append(
            f"{len(fresh_rows)} of {classified_count} classified clients hold fresh "
            f"evidence of a full view set while isolated ({names}); delivery "
            f"reached beyond the eligibility geometry"
        )
    residue = residue_rows(row for row in bound_rows if not row.isolated)
    return BreadthCensus(
        total=total,
        session_ok_count=session_ok_count,
        classified_clients=classified_count,
        isolated_clients=isolated_count,
        eligible_clients=classified_count - isolated_count,
        selected_count=selected,
        selected_head_count=selected_head,
        observed_but_isolated_fresh_clients=len(fresh_rows),
        unbound_clients=total - classified_count,
        late_bootstrapped_clients=sum(1 for row in bound_rows if row.late_bootstrapped),
        stalled_clients=sum(1 for row in rows if row.stalled),
        incomplete_evidence_clients=sum(1 for row in rows if row.incomplete),
        sparse_clients=sum(1 for entry in residue if entry.klass == "sparse"),
        never_opened_clients=sum(1 for entry in residue if entry.klass == "never_opened"),
        sparse_rows=residue,
        evidence_span_ns_min=evidence_spans[0] if evidence_spans else None,
        evidence_span_ns_p50=(evidence_spans[len(evidence_spans) // 2] if evidence_spans else None),
        violation="; ".join(violations) or None,
        attribution=tuple(
            BreadthAttribution(
                name=row.name,
                klass=isolation_cause(row),
                fresh=row.fresh,
            )
            for row in isolated_rows
        ),
        instance_index=instance_index,
    )


async def probe_breadth_rows(
    orch: EvidenceReader,
    records: Sequence[ClientRecord],
    rows: list[BreadthRow],
    min_distinct: int,
) -> None:
    """Fill session and view truth into the census rows.

    Session truth comes from the orchestrator's client registry (a client
    counts toward ``session_ok`` when its connection survived with a READY
    bootstrap); the observed reading comes from the client's sealed
    movement-window evidence, with the stall guard folded in. Runs after
    the binding re-stamp so each row's self-filter sees the bound actor id.
    """
    for rec, row in zip(records, rows, strict=True):
        status = await orch.client_state(rec.name)
        row.connected = status.connected
        row.session_ok = status.connected and status.bootstrap_state == BOOTSTRAP_READY
        row.observed, row.stalled, row.incomplete = await client_view_facts(orch, rec, min_distinct)
        window = await orch.view_window(rec.name)
        if window is not None:
            row.window_present = True
            row.distinct_in_window = distinct_non_self(window.distinct_ids, rec.own_actor_id)
            row.distinct_deque = distinct_non_self(window.deque_distinct_ids, rec.own_actor_id)


async def collect_breadth(
    orch: Orchestrator,
    records: list[ClientRecord],
    min_distinct: int,
    *,
    fresh_isolated_violates: bool = True,
) -> BreadthCensus:
    """Classify the collection-time bound cohort and contain the ratio.

    The binding map is re-read at collection time (the server's append-
    only allocation truth), so a client whose bootstrap completed after
    the pre-movement snapshot joins the cohort instead of vanishing from
    the denominator while its retained frames still count it as observed —
    the membership race that aborted healthy runs with ``selected`` above
    ``eligible``. Each bound client's own actor classifies against its
    instance's window-end roster exactly as the isolation classifier
    always has; the selected numerator ranges only over classified
    non-isolated clients, so containment holds by construction. Returns a
    census with the denominator counts ``None`` when no control plane
    exposes a roster: the numerator then keeps its legacy full-population
    reading.

    ``fresh_isolated_violates`` scopes the divergence guardrail to the
    topologies where its signal is meaningful: an isolated client whose
    window stream carried the full view set is divergence only where
    isolation implies cross-cell geometry. On the embedded single-cell
    topology an "isolated" client is pile-edge sparsity that legitimately
    receives the whole cell's publish (cell-scoped delivery),
    so the fresh flag renders there without breaching the guardrail; a
    multi-cell embedded profile would need the cell-aware form.
    """
    rows, classified = await prepare_breadth_rows(orch, records, min_distinct)
    return await finish_breadth(
        orch, records, rows, min_distinct, classified, fresh_isolated_violates
    )


async def collect_window_evidence(
    orch: Orchestrator,
    records: list[ClientRecord],
    min_distinct: int,
    *,
    fresh_isolated_violates: bool = True,
) -> tuple[BreadthCensus, EvidenceSnapshot]:
    """Bind the cohort, capture its observations, and reduce frozen breadth.

    The caller waits for the absolute collection deadlines first. Control
    queries retain their collection-time ordering; they are separate remote
    observations. The subsequent client capture does not yield or decode.
    The returned reader also supplies depth and delivered-frame reporting.
    """
    rows, classified = await prepare_breadth_rows(orch, records, min_distinct)
    evidence = orch.snapshot_clients(
        [rec.name for rec in records],
        event_clients=frozenset(rec.name for rec in records if rec.instrumented or rec.spread),
        retire_windows=True,
    )
    breadth = await finish_breadth(
        evidence, records, rows, min_distinct, classified, fresh_isolated_violates
    )
    return breadth, evidence


async def prepare_breadth_rows(
    orch: Orchestrator, records: list[ClientRecord], min_distinct: int
) -> tuple[list[BreadthRow], bool]:
    """Read collection-time bindings and rosters before observing clients."""
    control = orch.server_control
    rows = [
        BreadthRow(
            name=rec.name,
            instrumented=rec.instrumented,
            submitted=rec.inputs_submitted,
            connected=False,
            instance_index=rec.instance_index,
        )
        for rec in records
    ]
    rows_by_name = {row.name: row for row in rows}
    if control is None:
        return rows, False
    from tools.agent.distributed_host import DistributedControl

    groups: list[tuple[ServerControl, list[ClientRecord]]] = []
    if isinstance(control, DistributedControl):
        per_instance: dict[int, list[ClientRecord]] = {}
        for rec in records:
            if rec.instance_index < len(control.controls):
                per_instance.setdefault(rec.instance_index, []).append(rec)
        groups = [(control.controls[i], recs) for i, recs in sorted(per_instance.items())]
    else:
        groups = [(control, records)]
    for group_control, recs in groups:
        bindings = await principal_bindings(group_control)
        for rec in recs:
            if rec.principal_id not in bindings:
                continue
            row = rows_by_name[rec.name]
            row.bound = True
            if rec.own_actor_id == 0:
                row.late_bootstrapped = True
                rec.own_actor_id = bindings[rec.principal_id]
        positions = await roster_positions(group_control)
        iso_ids = isolated_actor_ids(
            positions,
            [rec.own_actor_id for rec in recs if rec.principal_id in bindings],
            min_distinct,
        )
        for rec in recs:
            if rec.principal_id in bindings:
                rows_by_name[rec.name].isolated = rec.own_actor_id in iso_ids
    return rows, True


async def finish_breadth(
    orch: EvidenceReader,
    records: list[ClientRecord],
    rows: list[BreadthRow],
    min_distinct: int,
    classified: bool,
    fresh_isolated_violates: bool,
) -> BreadthCensus:
    """Reduce the supplied client observations with the prepared cohort truth."""
    await probe_breadth_rows(orch, records, rows, min_distinct)
    if not classified:
        return assemble_with_instances(records, rows, classified=False)
    for rec, row in zip(records, rows, strict=True):
        if row.isolated:
            row.fresh = await view_evidence_fresh(orch, rec, min_distinct)
    spans: dict[str, int] = {}
    for rec in records:
        span = await orch.evidence_span_ns(rec.name)
        if span is not None:
            spans[rec.name] = span
    return assemble_with_instances(
        records,
        rows,
        classified=True,
        fresh_isolated_violates=fresh_isolated_violates,
        spans=spans,
    )


def assemble_with_instances(
    records: Sequence[ClientRecord],
    rows: Sequence[BreadthRow],
    *,
    classified: bool,
    fresh_isolated_violates: bool = True,
    spans: Mapping[str, int] | None = None,
) -> BreadthCensus:
    """Assemble the pooled census plus one census per instance cohort.

    The rows partition by each record's instance index and every partition
    reduces through the same :func:`_assemble_breadth` the pooled census
    uses, so a per-instance ratio reads exactly the pooled formula's slice
    of the population.
    """
    row_by_name = {row.name: row for row in rows}
    span_map = spans or {}
    census = assemble_breadth(
        len(records),
        rows,
        classified=classified,
        fresh_isolated_violates=fresh_isolated_violates,
        evidence_spans=sorted(span_map.values()),
    )
    rows_by_instance: dict[int, list[BreadthRow]] = {}
    for rec in records:
        rows_by_instance.setdefault(rec.instance_index, []).append(row_by_name[rec.name])
    instances = tuple(
        assemble_breadth(
            len(part_rows),
            part_rows,
            classified=classified,
            fresh_isolated_violates=fresh_isolated_violates,
            evidence_spans=sorted(span_map[row.name] for row in part_rows if row.name in span_map),
            instance_index=index,
        )
        for index, part_rows in sorted(rows_by_instance.items())
    )
    return replace(census, instances=instances)


async def publish_rate_counts(orch: EvidenceReader, records: list[ClientRecord]) -> dict[int, int]:
    """Sum the replication frames each instance delivered to its booted clients.

    For each instance, sums the dispatched replication-frame counts of
    every booted client on that instance. The per-client counter covers
    every frame the connection dispatched, so it stays exact where a
    history scan saturates at the client's retained-history bound. The
    integer per-instance counts are the poolable form of the publish-rate
    metric: aggregating several drives means summing these per instance,
    never averaging ratios.
    """
    per_instance: dict[int, int] = {}
    for rec in records:
        if rec.own_actor_id == 0 or rec.ready_ns <= rec.started_ns:
            continue
        count = await orch.replication_frame_count(rec.name)
        per_instance[rec.instance_index] = per_instance.get(rec.instance_index, 0) + count
    return per_instance


async def measure_publish_rate_skew(orch: EvidenceReader, records: list[ClientRecord]) -> float:
    """Return the max/min ratio of per-instance delivered-frame counts.

    Under even spread the ratio trends to 1.0; an instance that is the
    fixed publish bottleneck drives its clients' delivered-frame count
    down, raising the skew. Returns 0.0 when no booted client contributes
    (nothing to compare); booted clients that observed no frames at all
    drive the ratio to infinity (see :func:`_publish_rate_counts` for the
    underlying sums).
    """
    per_instance = await publish_rate_counts(orch, records)
    if not per_instance:
        return 0.0
    low = min(per_instance.values())
    if low == 0:
        return float("inf")
    return max(per_instance.values()) / low


BreadthRow.__module__ = "tools.agent.load_harness"
