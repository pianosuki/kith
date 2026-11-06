"""Measurement bounds and client retention budgets for scaling runs."""

from __future__ import annotations

from tools.agent.server_control import ServerControlError


# Bootstrap FSM state KITH_CLIENT_BOOTSTRAP_READY (include/kith/client/client.h).
BOOTSTRAP_READY: int = 2

# Movement input magnitude (Q16.16 normalized component). Calibrated to the
# tile2d model's speed so a moving actor stays within a radius-1 cell
# neighborhood long enough for the observer stream to record continuity
# samples (mirrors the movement_quality scenario calibration).
MOVE_INPUT: int = 200

# Submit rounds interleave a manual yield after this many clients so a
# round that outgrows its time slot cannot starve inbound processing.
SUBMIT_YIELD_EVERY: int = 512

# How many clients are deeply instrumented (large event history) for the
# metrics that need the full movement-window stream (move_missing,
# continuity_flicker). The remaining clients carry the default small
# history and contribute only to the breadth metrics (session_ok,
# selected_clients). Kept small so the dense-1000 run's memory stays
# bounded; 32 is statistically meaningful for the continuity metric
# (binary: flicker / no flicker) while catching rarer drops a 16-sample
# window misses.
INSTRUMENTED_SAMPLE_SIZE: int = 32

# Spread-probe cohort size: extra depth-instrumented clients at evenly
# spaced submit positions after the head sample. The head prefix is first
# in every submit round, so a pool whose accept/drop boundary lands past
# it protects the sample while the population absorbs the drops; the
# spread cohort measures the remaining positions. Diagnostic only — the
# gate metrics stay computed over the head sample.
SPREAD_PROBE_SIZE: int = 32

# Event-history depth for the instrumented sample: large enough to hold the
# full movement-window replication stream (a 5 s run at 20 Hz produces ~100
# input ticks and a multiple of that in delivered frames; 8192 covers it
# with margin for longer runs).
INSTRUMENTED_HISTORY_SIZE: int = 8192

# Event-history depth for the bulk (non-instrumented) clients. The breadth
# metrics (session_ok, selected_clients) only need a recent window: a
# connected subscriber receives a composed batch every refresh, so a few
# frames suffice to observe the view set. Sizing the bulk deque down from
# the instrumented depth bounds the per-client retained payload memory so
# the 2000-client gate fits in the remote host's RSS (the full-depth deque
# across every client OOMs at ~60 GB; the split drops it to ~12 GB).
BULK_HISTORY_SIZE: int = 64

# A gap between consecutive own-actor state frames longer than this during
# continuous input submission is a continuity flicker (the actor was
# visible, dropped, then resumed while still in view and still moving).
# Three sim ticks at 20 Hz = 150 ms; one tick of delivery slack brings
# the threshold to 200 ms. A gap at or above this means at least three
# consecutive publish cycles produced no observable self-frame.
FLICKER_GAP_MS: float = 200.0

# Post-window echo drain: the echo pipeline (sim apply through compose to
# client delivery) lags the movement window's end by up to the tiered
# max-gap backstop (1 s) plus one delivery cadence, so a depth scrape run
# at window end counts that lag as move-missing. The settle holds the
# scrape for this fixed bound, making the metric's operational definition
# — no observable self-update within the movement window plus the drain —
# identical across runs. It is a definition, not a tunable: the CLI
# override exists for the drain A/B diagnostic only and the report renders
# whichever value drove the run.
POST_WINDOW_ECHO_SETTLE_S: float = 2.0

# Breadth-window drain: the movement-window view accumulator seals at the
# window's end plus this bound, so a frame in flight at window end counts
# toward the client's breadth evidence while the post-window max-gap
# backstop waves (observed landing ~0.9 s after window end, then ~1/s
# while the world is quiescent) stay sealed out. The depth channel keeps
# its own longer echo drain (``_POST_WINDOW_ECHO_SETTLE_S``): depth asks
# "which submitted inputs did the source certify within the window plus
# the full echo lag", breadth asks "what view set arrived within the
# window plus one delivery cadence" — the two channels share the window
# bounds but deliberately not the drain.
BREADTH_WINDOW_DRAIN_S: float = 0.5

# Breadth-window stall bound: the largest receipt gap between consecutive
# in-window replication frames a healthy client's stream may show. Derived
# from the delivery arithmetic — the tiered max-gap backstop (1 s) plus one
# tier cadence (0.5 s) plus scheduler slack (0.5 s); the certified runs'
# census measured a worst observed gap of 209 ms, ~9x below the bound. A
# client whose in-window frames gap past the bound suffered a mid-window
# stream stall: even if its early frames carried the view bar, the stream
# that died did not deliver a meaningful view set, so the client is not
# selected. The continuity-flicker check (200 ms, own-actor frames) is a
# different question on a different stream with its own bound.
BREADTH_WINDOW_MAX_GAP_S: float = 2.0

# Per-client byte budget for the breadth-evidence deque: summary clients
# retain every replication frame's raw payload until the deque's total
# bytes exceed this, then evict oldest-first. The deque is both the
# forensic payload record and the breadth probe's counting basis: the
# movement-window view read (:meth:`view_window`) walks the frames whose
# arrival timestamp falls inside the window bounds and extracts ids at
# read time, so the ingest path carries no per-frame window work — even a
# small ingest hook measurably perturbs the driver/server cadence at the
# distributed point. The budget must hold the whole window at the
# certified cadence: the batch replication frames average ~5.7 KB, so
# the 5.2 s window-plus-drain needs ~600 KB at the average cadence, and
# the heaviest per-client window streams on the certified distributed
# drive run past 2 MiB. 8 MiB carries ~3x headroom over that measured
# worst. The window read
# answers any residual eviction loudly: a frame evicted from under the
# window marks the client's evidence incomplete and the census reports
# it instead of trusting a shrunken basis. The O(1) span read renders
# the retention margin, and census-class post-mortem tooling decodes the
# deque after a run to reconstruct what the stream actually carried (the
# per-frame census that root-caused the probe-moment race read exactly
# this deque).
BREADTH_EVIDENCE_BUDGET_BYTES: int = 8 * 1024 * 1024

# Poll cadence for bootstrap-latency sampling and the movement-phase
# deadline checks.
POLL_INTERVAL_S: float = 0.02

# Movement-window offset between the two drivers in dual-driver mode when
# --driver-stagger-ms is not given: half the observed inter-cluster period,
# large enough that the two windows cannot be mistaken for coherent.
DEFAULT_STAGGER_MS: int = 600

# Exception group caught at every /query_state probe. Defined once as a
# tuple so ruff format does not strip the grouping (it removes parentheses
# from `except (A, B):` because Python 3.14 accepts the bare-comma tuple
# form, but mypy 2.3 flags that form as a syntax error; a single name
# avoids the conflict). ServerControlError covers HTTP status and
# transport failures from the control-plane client; the harness degrades
# to an empty roster so the depth metrics fall back to the instrumented
# sample rather than aborting the bootstrap.
QUERY_STATE_ERRORS = (RuntimeError, OSError, ValueError, ServerControlError)

# Loud guardrail on breadth eligibility: when more than this fraction of
# the classified clients count as isolated, the roster geometry itself is
# untrustworthy (a position/placement defect, or a cell-size/radius
# divergence between the server and the classifier) and exempting that
# many clients from the selected_clients denominator silently lowers
# the effective bar. The corridor movement pattern drives a natural
# leading-edge isolation above this bound at full population, so a trip
# here fails the run's verdict with its measured values rendered while
# collection continues; it never aborts measurement mid-drive.
ISOLATED_FRACTION_MAX: float = 0.05
