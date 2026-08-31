"""Harness-side /proc scheduler-statistics sampler for gate diagnostics.

Samples per-thread CPU time and context-switch counters from ``/proc``
for a set of target PIDs (the harness process itself plus, in
subprocess-cluster mode, each server instance) on a fixed wall-clock
cadence, and records the raw cumulative counters per sample. All
deltas, baselines, and verdicts are computed in a separate analysis pass
over the lossless trace (:mod:`tools.agent.gap_ledger`), never during
collection.

The trace discriminates continuity-flicker gap windows by what the
kernel accounted on each server thread while a gap was open:

- rising ``nonvoluntary_ctxt_switches`` with low CPU: the thread was
  runnable but preempted (scheduling contention; the affinity lever);
- rising ``voluntary_ctxt_switches`` with low CPU: the thread chose to
  block in the kernel (futex/io_uring wait; an in-server stall);
- CPU time near or above half the window wall: real work, not waiting;
- flat switches with low CPU: mostly asleep (nothing pending).

In each server subprocess the tick/reactor loop runs on the thread
named ``kith-server-run`` (``kith_server_run`` drives
``kith_reactor_run`` on that worker); the process main thread parks in
the shutdown-signal wait and accounts almost nothing. Attribution
therefore selects the tick row by that comm, not by ``tid == pid``
(:mod:`tools.agent.gap_ledger`). All timestamps come from
``time.monotonic_ns()`` — the same clock base as client event receipts
and movement-window bounds — so samples join directly against gap
windows.
"""

from __future__ import annotations

import os
import re
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass


__all__ = [
    "ProcSample",
    "ProcSampler",
    "ProcTrace",
    "SamplerTarget",
    "TargetSample",
    "ThreadSample",
]


@dataclass(frozen=True, slots=True)
class SamplerTarget:
    """One sampled PID identified by a report-facing name."""

    name: str
    pid: int


@dataclass(frozen=True, slots=True)
class ThreadSample:
    """Cumulative kernel accounting for one target thread.

    ``utime_ticks`` / ``stime_ticks`` are the user / system cpu times in
    ``SC_CLK_TCK`` clock ticks from ``/proc/<pid>/task/<tid>/stat``;
    ``nvcsw`` / ``nivcsw`` are the voluntary / nonvoluntary
    context-switch counts from ``/proc/<pid>/task/<tid>/status``.
    """

    tid: int
    comm: str
    utime_ticks: int
    stime_ticks: int
    nvcsw: int
    nivcsw: int

    @property
    def cpu_ticks(self) -> int:
        """Total cpu time in ticks: user plus system."""
        return self.utime_ticks + self.stime_ticks


@dataclass(frozen=True, slots=True)
class TargetSample:
    """One target's thread rows at one sample instant.

    ``threads`` is empty when the pid vanished mid-run (a crashed or
    already-reaped subprocess); an absent target reads as zeros at
    analysis time rather than aborting the trace.
    """

    target: str
    pid: int
    threads: tuple[ThreadSample, ...]


@dataclass(frozen=True, slots=True)
class ProcSample:
    """One sampling instant across every configured target."""

    t_ns: int
    targets: tuple[TargetSample, ...]


@dataclass(frozen=True, slots=True)
class ProcTrace:
    """The full sample series of one run, plus its units.

    ``clk_tck`` converts ``ThreadSample.cpu_ticks`` to seconds at
    analysis time; capturing it once at sampler construction pins the
    unit even when the analysis runs on another host.
    """

    interval_s: float
    clk_tck: int
    samples: tuple[ProcSample, ...]


_NVCSW_RE = re.compile(r"^voluntary_ctxt_switches\s*:\s*(\d+)\s*$", re.MULTILINE)
_NIVCSW_RE = re.compile(r"^nonvoluntary_ctxt_switches\s*:\s*(\d+)\s*$", re.MULTILINE)


def _read_text(path: str) -> str | None:
    """Read a small procfs file, returning None when it vanishes mid-read."""
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            return handle.read()
    except OSError:
        return None


def _parse_stat(stat_text: str) -> tuple[str, list[str]] | None:
    """Split one ``/proc/<pid>/stat`` line into ``(comm, tail_fields)``.

    The comm field is wrapped in parentheses and may itself contain
    spaces and parentheses, so the split anchors on the first ``(`` and
    the LAST ``)``; ``tail_fields[0]`` is stat field 3 (state). Returns
    None for a malformed line.
    """
    opening = stat_text.find("(")
    closing = stat_text.rfind(")")
    if opening == -1 or closing == -1 or closing < opening:
        return None
    comm = stat_text[opening + 1 : closing]
    tail = stat_text[closing + 1 :].split()
    if len(tail) < 13:
        return None
    return comm, tail


def _thread_sample_from_texts(tid: int, stat_text: str, status_text: str) -> ThreadSample | None:
    """Parse one thread's cumulative counters from its stat + status texts."""
    parsed = _parse_stat(stat_text)
    if parsed is None:
        return None
    comm, tail = parsed
    nvcsw = _NVCSW_RE.search(status_text)
    nivcsw = _NIVCSW_RE.search(status_text)
    if nvcsw is None or nivcsw is None:
        return None
    return ThreadSample(
        tid=tid,
        comm=comm,
        # tail[0] is state (field 3); utime/stime are fields 14/15.
        utime_ticks=int(tail[11]),
        stime_ticks=int(tail[12]),
        nvcsw=int(nvcsw.group(1)),
        nivcsw=int(nivcsw.group(1)),
    )


def _read_thread_sample(pid: int, tid: int) -> ThreadSample | None:
    """Read one thread's cumulative counters; None when either file is gone."""
    stat = _read_text(f"/proc/{pid}/task/{tid}/stat")
    status = _read_text(f"/proc/{pid}/task/{tid}/status")
    if stat is None or status is None:
        return None
    return _thread_sample_from_texts(tid, stat, status)


def _sample_target(target: SamplerTarget) -> TargetSample:
    """Read every live thread of one target pid."""
    try:
        task_ids = os.listdir(f"/proc/{target.pid}/task")
    except OSError:
        task_ids = []
    threads: list[ThreadSample] = []
    for entry in sorted(task_ids):
        if not entry.isdigit():
            continue
        sample = _read_thread_sample(target.pid, int(entry))
        if sample is not None:
            threads.append(sample)
    return TargetSample(target=target.name, pid=target.pid, threads=tuple(threads))


class ProcSampler:
    """Background sampler recording raw /proc counters at a fixed cadence.

    The sampler runs on a daemon thread so the blocking procfs reads
    never perturb the harness's asyncio loop timing. Samples accumulate
    in a plain list written only by that thread; :meth:`stop` joins the
    thread before returning the trace, which hands the list back under
    a happens-before edge — no lock is needed for the stop-then-read
    discipline the harness uses.
    """

    def __init__(self, targets: Sequence[SamplerTarget], interval_s: float) -> None:
        self._targets = tuple(targets)
        self._interval_s = interval_s
        self._clk_tck = os.sysconf("SC_CLK_TCK")
        self._samples: list[ProcSample] = []
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name="kith-proc-sampler", daemon=True)

    @classmethod
    def start(cls, targets: Sequence[SamplerTarget], *, interval_s: float = 0.1) -> ProcSampler:
        """Construct and start a sampler over ``targets``.

        Args:
            targets: PIDs to sample (server instances plus the harness
                itself).
            interval_s: Cadence in seconds; 100 ms bounds the worst-case
                miss against a ~200 ms gap window.

        Returns:
            The started sampler. Call :meth:`stop` exactly once to end
            it and receive the trace.
        """
        sampler = cls(targets, interval_s)
        sampler._thread.start()
        return sampler

    def stop(self) -> ProcTrace:
        """Stop the sampler, join its thread, and return the full trace."""
        self._stop_event.set()
        self._thread.join()
        return ProcTrace(
            interval_s=self._interval_s,
            clk_tck=self._clk_tck,
            samples=tuple(self._samples),
        )

    def samples_collected(self) -> int:
        """Return a lower bound on the samples the writer has appended.

        Readable from any thread while the sampler runs; a concurrent
        read is never an overcount. The trace from :meth:`stop` remains
        the complete, ordered handoff.
        """
        return len(self._samples)

    def _run(self) -> None:
        """Sample until stopped, holding the cadence from each instant start."""
        while not self._stop_event.is_set():
            began = time.monotonic()
            try:
                sample = ProcSample(
                    t_ns=time.monotonic_ns(),
                    targets=tuple(_sample_target(target) for target in self._targets),
                )
            except OSError:
                sample = None
            if sample is not None:
                self._samples.append(sample)
            elapsed = time.monotonic() - began
            self._stop_event.wait(max(0.0, self._interval_s - elapsed))
