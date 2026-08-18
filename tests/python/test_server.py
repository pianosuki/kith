"""Unit tests for the public :class:`kith.Server` facade.

The tests point ``KITH_LIB`` at the debug build directory so the ctypes bridge
loads the freshly built shared libraries, then reset the bridge singleton
around each test so cached state from a prior test cannot leak in. The
lifecycle cases mirror the C server run tests (``tests/c/test_server_run.c``)
through the Python boundary: shutdown-before-run returns without entering the
reactor loop, shutdown is idempotent, and the blocking run loop exits once a
peer thread requests shutdown.

The registration surface (``register_message_handler``, ``register_sim_model``,
``register_zone``, ``register_query``, ``register_control_route``) calls the
plane functions on the server's borrowed plane handles directly. The tests
cover the positive paths (a handler registers, a built-in model instantiates
and steps, a zone id is reserved, a control route registers) and the negative
paths (registering a query without a persistence pool raises
``KithStateError``, registering a duplicate zone raises ``KithError``).
"""

from __future__ import annotations

import os
import signal
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from types import FrameType

import pytest
from _build_gate import _BUILD_DEBUG, needs_build

from kith import Config, KithError, Server, ServerStatus, Topology
from kith._bridge import reset
from kith._generated import types as gen_types
from kith.exceptions import KithStateError


def _new_server_with_retry(attempts: int = 10) -> Server:
    # Each server owns a reactor ring; ring pages are memcg-accounted kernel
    # memory that reclaims asynchronously after queue_exit, so cycling
    # servers back-to-back can transiently pressure the cgroup memory
    # budget. A bounded retry rides out the reclaim window.
    for attempt in range(attempts):
        try:
            return Server()
        except KithError as exc:
            if attempt == attempts - 1 or exc.code.name not in ("KITH_EIO", "KITH_ENOMEM"):
                raise
            time.sleep(0.05)
    raise AssertionError("unreachable")


def _status(server: Server) -> ServerStatus:
    """Read the handle state through a fresh call.

    Routing through a function returns the full ``ServerStatus`` type each
    time, so mypy does not carry narrowing from an earlier ``is`` assert on
    ``server.status`` across a mutating call (``shutdown`` / ``close``) that
    the property reflects.
    """
    return server.status


def _tiered_config_bytes() -> bytes:
    """Return a well-formed tiered delivery config image for the boundary tests.

    The image is assembled from the generated ctypes struct so its layout
    tracks the ABI exactly as production callers (via the public builder)
    receive it.
    """
    import ctypes

    from kith._generated import gateway as gen_gateway
    from kith._generated import version as gen_version

    config = gen_gateway.kith_gateway_tiered_config_t(
        size=ctypes.sizeof(gen_gateway.kith_gateway_tiered_config_t),
        abi_version=gen_version.KITH_ABI_VERSION,
    )
    return bytes(config)


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin KITH_LIB to the debug build dir and reset the singleton per test."""
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


@needs_build
class TestConstruction:
    def test_defaults_create_handle_in_created_state(self) -> None:
        server = Server()
        try:
            assert _status(server) is ServerStatus.CREATED
        finally:
            server.close()

    def test_embedded_topology_builds(self) -> None:
        server = Server(topology="embedded")
        try:
            assert _status(server) is ServerStatus.CREATED
        finally:
            server.close()

    def test_topology_enum_accepted(self) -> None:
        server = Server(topology=Topology.EMBEDDED)
        try:
            assert _status(server) is ServerStatus.CREATED
        finally:
            server.close()

    def test_unknown_topology_raises_einval(self) -> None:
        with pytest.raises(KithError) as exc_info:
            Server(topology="cluster")
        assert exc_info.value.code == gen_types.kith_error.KITH_EINVAL

    def test_base_exception_for_ambiguous_code(self) -> None:
        # KITH_EINVAL has no single family; it raises the base KithError.
        with pytest.raises(KithError) as exc_info:
            Server(topology="cluster")
        assert exc_info.value.code == gen_types.kith_error.KITH_EINVAL
        assert type(exc_info.value) is KithError

    def test_server_accepts_config_instance(self, tmp_path: Path) -> None:
        env_file = tmp_path / "game.env"
        env_file.write_text("tick_hz = 25\n", encoding="utf-8")
        with Config(file_path=env_file) as cfg, Server(config=cfg) as server:
            assert server.status is ServerStatus.CREATED
        # After the server closes, the borrowed Config is still alive (the
        # caller owns it) and reports its key. cfg is closed by its own
        # context manager here, not by the server.

    def test_delivery_strategy_builds_handle(self) -> None:
        server = Server(topology="embedded", delivery_strategy="tiered")
        try:
            assert _status(server) is ServerStatus.CREATED
        finally:
            server.close()

    def test_delivery_config_image_accepted(self) -> None:
        image = _tiered_config_bytes()
        server = Server(
            topology="embedded",
            delivery_strategy="tiered",
            delivery_config=image,
        )
        try:
            assert _status(server) is ServerStatus.CREATED
        finally:
            server.close()

    def test_undeclared_delivery_config_rejected(self) -> None:
        # A size-versioned image whose leading uint32 declares length 0
        # fails the copy-time sanity range with EINVAL.
        with pytest.raises(KithError) as exc_info:
            Server(
                topology="embedded",
                delivery_strategy="tiered",
                delivery_config=b"\x00\x00\x00\x00",
            )
        assert exc_info.value.code == gen_types.kith_error.KITH_EINVAL

    def test_topology_is_case_insensitive(self) -> None:
        server = Server(topology="EMBEDDED")
        try:
            assert _status(server) is ServerStatus.CREATED
        finally:
            server.close()

    def test_config_from_env_file_builds_source(self, tmp_path: Path) -> None:
        env_file = tmp_path / "game.env"
        env_file.write_text("# kith config\ntick_hz = 30\nlisten_port = 7778\n", encoding="utf-8")
        server = Server(config=env_file)
        try:
            assert _status(server) is ServerStatus.CREATED
        finally:
            server.close()

    def test_config_from_missing_file_is_not_an_error(self, tmp_path: Path) -> None:
        # A missing file is not an error per kith_config_create: the source is
        # built from nothing and every plane takes its defaults.
        server = Server(config=tmp_path / "absent.env")
        try:
            assert _status(server) is ServerStatus.CREATED
        finally:
            server.close()

    def test_config_from_unreadable_path_raises_eio(self, tmp_path: Path) -> None:
        # Passing a directory as file_path fails the read and reports EIO.
        with pytest.raises(KithError) as exc_info:
            Server(config=tmp_path)
        assert exc_info.value.code == gen_types.kith_error.KITH_EIO

    def test_listen_port_is_assigned_for_ephemeral_binding(self) -> None:
        # The default listen_port (0) binds an OS-assigned ephemeral port; the
        # facade reads the actual port back from the C handle so a caller that
        # bound ephemerally knows the real endpoint.
        server = Server()
        try:
            assert server.listen_port != 0
        finally:
            server.close()

    def test_listen_port_is_zero_after_close(self) -> None:
        server = Server()
        assert server.listen_port != 0
        server.close()
        assert server.listen_port == 0

    def test_listen_host_pinned_binding(self) -> None:
        # An explicit listen_host pins the gateway bind to that address; the
        # server boots normally on the loopback pin.
        server = Server(listen_host="127.0.0.1")
        try:
            assert _status(server) is ServerStatus.CREATED
            assert server.listen_port != 0
        finally:
            server.close()

    def test_listen_host_unresolvable_raises_eio(self) -> None:
        # An unresolvable host name fails the create at the transport
        # listener; kith_net_listen reports every listener-establishment
        # failure, resolution included, as EIO.
        with pytest.raises(KithError) as exc_info:
            Server(listen_host="bogus.invalid")
        assert exc_info.value.code == gen_types.kith_error.KITH_EIO

    def test_config_keys_fill_unset_args(self, tmp_path: Path) -> None:
        # The wiring reads the documented keys for arguments left at their
        # defaults; the bound-port readback proves the file value reached
        # the transport listener.
        env_file = tmp_path / "game.env"
        env_file.write_text("tick_hz = 30\nlisten_port = 7779\n", encoding="utf-8")
        server = Server(config=env_file)
        try:
            assert server.listen_port == 7779
        finally:
            server.close()

    def test_config_keys_explicit_args_win(self, tmp_path: Path) -> None:
        # An explicit listen_port argument beats the config key.
        env_file = tmp_path / "game.env"
        env_file.write_text("listen_port = 7779\n", encoding="utf-8")
        server = Server(config=env_file, listen_port=7781)
        try:
            assert server.listen_port == 7781
        finally:
            server.close()

    def test_config_bad_value_raises_einval(self, tmp_path: Path) -> None:
        # A present key whose value fails its parse fails the create instead
        # of silently falling back to the default.
        env_file = tmp_path / "game.env"
        env_file.write_text("tick_hz = thirty\n", encoding="utf-8")
        with pytest.raises(KithError) as exc_info:
            Server(config=env_file)
        assert exc_info.value.code == gen_types.kith_error.KITH_EINVAL

    def test_control_port_is_assigned_for_ephemeral_binding(self) -> None:
        # The composition root starts the control plane on an OS-assigned
        # ephemeral port; the facade reads the bound port back so a caller
        # can reach the HTTP routes registered via register_control_route.
        server = Server()
        try:
            assert server.control_port != 0
        finally:
            server.close()

    def test_control_port_is_zero_after_close(self) -> None:
        server = Server()
        assert server.control_port != 0
        server.close()
        assert server.control_port == 0


@needs_build
class TestLifecycle:
    def test_shutdown_before_run_transitions_to_stopped(self) -> None:
        server = Server()
        try:
            assert _status(server) is ServerStatus.CREATED
            server.shutdown()
            assert _status(server) is ServerStatus.STOPPED
            # run on a stopped handle returns without entering the reactor loop.
            server.run()
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.close()

    def test_shutdown_is_idempotent(self) -> None:
        server = Server()
        try:
            server.shutdown()
            server.shutdown()
            server.shutdown()
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.close()

    def test_run_returns_after_shutdown_from_thread(self) -> None:
        server = Server()
        try:
            stop = threading.Event()

            def request_shutdown() -> None:
                # Let the run loop enter the reactor before requesting drain.
                time.sleep(0.05)
                server.shutdown()
                stop.set()

            thread = threading.Thread(target=request_shutdown)
            thread.start()
            # Blocks until the tick callback observes the shutdown flag.
            server.run()
            thread.join(timeout=5.0)
            assert stop.is_set(), "shutdown thread did not complete"
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.close()

    def test_close_blocks_until_run_exits_without_manual_shutdown(self) -> None:
        # close() on a server whose run loop is in flight on another thread
        # requests shutdown and blocks until kith_server_run returns before
        # destroying the handle, so the caller does not need to call
        # shutdown() and join the run thread manually. The C runtime abort
        # in kith_server_destroy is never reached because close() waits.
        server = Server()
        run_thread = threading.Thread(target=server.run, daemon=True)
        run_thread.start()
        # Let the run loop enter the reactor before close() is called.
        time.sleep(0.05)
        # close() shuts the loop down and joins the run; it must not raise and
        # must leave the facade closed.
        server.close()
        run_thread.join(timeout=5.0)
        assert not run_thread.is_alive(), "run thread did not exit on close"
        assert _status(server) is ServerStatus.STOPPED

    def test_close_racing_run_startup_never_orphans_the_loop(self) -> None:
        # Hammers close() against a run thread that has not announced itself
        # yet: without the lifecycle lock, a close that decides "no run in
        # flight" destroys the handle under the not-yet-entered
        # kith_server_run (thread exceptions or use-after-free). Every
        # interleaving must now end with the run joined, and a run that lost
        # the race may only surface KithStateError.
        stray: list[BaseException] = []

        def _hook(args: threading.ExceptHookArgs) -> None:
            if args.exc_value is not None:
                stray.append(args.exc_value)

        previous = threading.excepthook
        threading.excepthook = _hook
        try:
            for i in range(8):
                server = _new_server_with_retry()
                run_thread = threading.Thread(target=server.run, daemon=True)
                run_thread.start()
                server.close()
                run_thread.join(timeout=10.0)
                assert not run_thread.is_alive(), f"run outlived close (iteration {i})"
        finally:
            threading.excepthook = previous
        unexpected = [exc for exc in stray if not isinstance(exc, KithStateError)]
        assert not unexpected, f"unexpected thread exceptions: {unexpected!r}"

    def test_run_after_close_raises_state_error(self) -> None:
        server = Server()
        server.close()
        with pytest.raises(KithStateError):
            server.run()


@needs_build
class TestServe:
    def test_sigterm_ends_serve_and_close_joins(self) -> None:
        # Pre-block both signals BEFORE the facade constructs the worker
        # pool: threads inherit the creating thread's mask, and a SIGTERM
        # delivered to any unblocked thread ends the test process by
        # default disposition instead of pending for serve()'s wait. The
        # pool threads block both from entry themselves; the timer thread
        # here inherits the mask set below.
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
        try:
            server = Server()
            killer = threading.Timer(0.3, os.kill, args=(os.getpid(), signal.SIGTERM))
            killer.start()
            try:
                server.serve()
            finally:
                killer.cancel()
            # serve() returns on delivery; the drain finishes on its worker
            # and close() waits for it before destroying the handle.
            server.close()
            assert _status(server) is ServerStatus.STOPPED
        finally:
            # serve() consumed the delivered signal, so restoring the mask
            # exposes no pending default-disposition kill to the suite.
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)

    def test_serve_rejects_when_run_in_flight(self) -> None:
        server = Server()
        try:
            run_thread = threading.Thread(target=server.run, daemon=True)
            run_thread.start()
            time.sleep(0.05)
            with pytest.raises(KithError):
                server.serve()
            server.shutdown()
            run_thread.join(timeout=5.0)
        finally:
            server.close()

    def test_serve_on_closed_handle_raises(self) -> None:
        server = Server()
        server.close()
        with pytest.raises(KithError):
            server.serve()


@needs_build
class TestRunSignalPump:
    """run() on the interpreter main thread receives signals via the pump.

    CPython runs signal handlers only in the main thread's eval loop, which
    sits inside the blocking C run call here; the registered poll observer
    pumps them once per tick (the bounded exception to the tick path's
    no-Python rule).
    """

    def _kill_when_running(self, server: Server, signum: int) -> threading.Timer:
        # Deliver the signal only once the loop is up, so the pump is what
        # observes it rather than the pre-run setup window.
        def _fire() -> None:
            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            time.sleep(0.05)
            os.kill(os.getpid(), signum)

        timer = threading.Timer(0.0, _fire)
        timer.start()
        return timer

    def _install_disposition(
        self,
        signum: int,
        handler: Callable[[int, FrameType | None], None] | int | signal.Handlers,
    ) -> Callable[[int, FrameType | None], None] | int | signal.Handlers | None:
        """Set a signal disposition and return the previous one."""
        previous = signal.getsignal(signum)
        signal.signal(signum, handler)
        return previous

    def _restore_disposition(
        self,
        signum: int,
        previous: Callable[[int, FrameType | None], None] | int | signal.Handlers | None,
    ) -> None:
        # A None read means the handler was never set from Python; SIG_DFL
        # is the restorable equivalent.
        signal.signal(signum, previous if previous is not None else signal.SIG_DFL)

    def test_sigint_with_default_handler_drains_without_exception(
        self,
    ) -> None:
        # The default int handler raises KeyboardInterrupt inside the pump's
        # CheckSignals call; the exception cannot cross the C run loop, so the
        # pump drops it and requests the drain instead. run() returns cleanly.
        previous = self._install_disposition(signal.SIGINT, signal.default_int_handler)
        server = Server()
        killer = self._kill_when_running(server, signal.SIGINT)
        try:
            server.run()
        finally:
            killer.cancel()
            killer.join(timeout=5.0)
            self._restore_disposition(signal.SIGINT, previous)
        assert _status(server) is ServerStatus.STOPPED
        server.close()

    def test_sigterm_at_default_disposition_drains_via_shim(self) -> None:
        # SIG_DFL for SIGTERM kills the process outright; the pump fits a
        # graceful-shutdown shim for the duration of the run and restores the
        # disposition afterwards.
        previous = self._install_disposition(signal.SIGTERM, signal.SIG_DFL)
        server = Server()
        killer = self._kill_when_running(server, signal.SIGTERM)
        try:
            server.run()
        finally:
            killer.cancel()
            killer.join(timeout=5.0)
            self._restore_disposition(signal.SIGTERM, previous)
        assert _status(server) is ServerStatus.STOPPED
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL
        server.close()

    def test_user_signal_handler_runs_and_can_request_shutdown(self) -> None:
        # A disposition the embedder set is left in place; the pump invokes
        # it, and the handler's own re-entrant FFI call to shutdown() (from
        # inside the observer trampoline) drives the drain.
        received: list[int] = []
        server_ref: list[Server] = []

        def _handler(signum: int, frame: FrameType | None) -> None:
            del frame
            received.append(signum)
            server_ref[0].shutdown()

        previous = self._install_disposition(signal.SIGTERM, _handler)
        server = Server()
        server_ref.append(server)
        killer = self._kill_when_running(server, signal.SIGTERM)
        try:
            server.run()
        finally:
            killer.cancel()
            killer.join(timeout=5.0)
            self._restore_disposition(signal.SIGTERM, previous)
        assert received == [signal.SIGTERM]
        assert _status(server) is ServerStatus.STOPPED
        # The finally block already restored the pre-run disposition.
        assert signal.getsignal(signal.SIGTERM) is previous
        server.close()

    def test_run_on_worker_thread_leaves_dispositions_untouched(self) -> None:
        # The pump is a main-thread-only affordance; a run on any other
        # thread registers no observer and replaces no disposition.
        before = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
        server = Server()
        try:
            run_thread = threading.Thread(target=server.run, daemon=True)
            run_thread.start()
            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            server.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.close()
        after = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
        assert after == before


@needs_build
class TestContextManager:
    def test_context_manager_closes_handle(self) -> None:
        with Server() as server:
            assert _status(server) is ServerStatus.CREATED
        assert _status(server) is ServerStatus.STOPPED

    def test_close_is_idempotent(self) -> None:
        server = Server()
        server.close()
        server.close()  # second close is a no-op

    def test_status_after_close_is_stopped(self) -> None:
        # close() releases the C handle; the facade reports STOPPED rather than
        # the C null-handle sentinel (CREATED) so a closed server reads as
        # inactive.
        server = Server()
        server.close()
        assert _status(server) is ServerStatus.STOPPED


@needs_build
class TestRegistrationSurface:
    # The gateway dispatches handlers by direct index on a table sized at
    # server create time, so a game using user type ids (>= 1000) must size
    # the table to cover them. The default (256) covers framework types.
    _USER_TYPE: int = 1001
    _TABLE_SIZE: int = 4096

    def test_register_message_handler_registers_on_borrowed_gateway(self) -> None:
        server = Server(handler_table_size=self._TABLE_SIZE)
        try:
            # Registering a second handler for the same type replaces the
            # first (the gateway table slot is overwritten); re-registration
            # answers 0, and the dispatch-level replacement proof lives in
            # the gateway C suite where frames really flow.
            server.register_message_handler(self._USER_TYPE, lambda t, p, s: None)
            server.register_message_handler(self._USER_TYPE, lambda t, p, s: None)
            assert server.status is ServerStatus.CREATED
        finally:
            server.close()

    def test_register_sim_model_instantiates_builtin_and_steps(self) -> None:
        from kith import Actor, SimModelConfig

        server = Server()
        try:
            model = server.register_sim_model("free2d", SimModelConfig())
            stepped = model.step([Actor(id=1, pos_x=0, pos_y=0)], dt_ms=50)
            assert len(stepped) == 1
            assert stepped[0].id == 1
            # A borrowed view's close is a no-op; the facade owns the model.
            model.close()
            assert server.status is ServerStatus.CREATED
        finally:
            server.close()

    def test_borrowed_model_view_close_keeps_facade_release_intact(self) -> None:
        from kith import Actor, SimModelConfig

        server = Server()
        try:
            model = server.register_sim_model("free2d", SimModelConfig())
            stepped = model.step([Actor(id=1, pos_x=0, pos_y=0)], dt_ms=50)
            assert len(stepped) == 1
            # A borrowed close is a no-op: the view stays usable and the
            # facade's release below must still reach the underlying handle.
            model.close()
            assert model.handle is not None
            stepped = model.step([Actor(id=1, pos_x=0, pos_y=0)], dt_ms=50)
            assert len(stepped) == 1
        finally:
            server.close()
        assert model.handle is None
        model.close()

    def test_register_sim_model_unknown_name_raises_enoent(self) -> None:
        from kith import SimModelConfig

        server = Server()
        try:
            with pytest.raises(KithError) as exc_info:
                server.register_sim_model("no_such_model", SimModelConfig())
            assert exc_info.value.code == gen_types.kith_error.KITH_ENOENT
        finally:
            server.close()

    def test_register_zone_reserves_monotonic_id(self) -> None:
        server = Server()
        try:
            first = server.register_zone("overworld")
            second = server.register_zone("dungeon")
            assert first == 1
            assert second == 2
        finally:
            server.close()

    def test_register_zone_duplicate_raises_eexist(self) -> None:
        server = Server()
        try:
            server.register_zone("overworld")
            with pytest.raises(KithError) as exc_info:
                server.register_zone("overworld")
            assert exc_info.value.code == gen_types.kith_error.KITH_EEXIST
        finally:
            server.close()

    def test_register_query_without_pool_raises_estate(self) -> None:
        from kith import KithStateError

        server = Server()
        try:
            with pytest.raises(KithStateError) as exc_info:
                server.register_query("sample_query", "SELECT 1", 0)
            assert exc_info.value.code == gen_types.kith_error.KITH_ESTATE
        finally:
            server.close()

    def test_register_control_route_registers_on_borrowed_control(self) -> None:
        server = Server()
        try:
            server.register_control_route("POST", "/command", lambda req, resp: None)
            assert server.status is ServerStatus.CREATED
        finally:
            server.close()

    def test_register_proto_type_registers_on_borrowed_proto(self) -> None:
        server = Server()
        try:
            server.register_proto_type("login", 1000)
            server.register_proto_type("actor_state", 1001)
            # Re-registering the same name at the same id is idempotent: the
            # registry holds one (name, id) pair, so re-registering it is not
            # a duplicate id-to-different-name conflict.
            server.register_proto_type("login", 1000)
            assert server.status is ServerStatus.CREATED
        finally:
            server.close()

    def test_register_proto_type_duplicate_id_conflict_raises_estate(self) -> None:
        from kith import KithStateError

        server = Server()
        try:
            server.register_proto_type("login", 1000)
            with pytest.raises(KithStateError) as exc_info:
                server.register_proto_type("login_reply", 1000)
            assert exc_info.value.code == gen_types.kith_error.KITH_EEXIST
        finally:
            server.close()

    def test_borrowed_model_released_on_close(self) -> None:
        from kith import Actor, SimModelConfig

        server = Server()
        model = server.register_sim_model("free2d", SimModelConfig())
        server.close()
        # After close the facade-owned model is destroyed; stepping on the
        # stale view raises (the underlying handle is gone) rather than
        # corrupting state silently.
        with pytest.raises(KithError):
            model.step([Actor(id=1)], 50)

    def test_apply_input_mints_update_seq_through_the_facade(self) -> None:
        from kith import Actor, SimInput, SimModelConfig

        server = Server()
        try:
            model = server.register_sim_model("free2d", SimModelConfig())
            first = model.apply_input(Actor(id=1), SimInput(input_tick=1, move_x=1000))
            assert first.update_seq == 1
            second = model.apply_input(first, SimInput(input_tick=2, move_x=1000))
            assert second.update_seq == 2
            # Stepping advances positions only; the counter rides unchanged.
            stepped = model.step([second], dt_ms=50)
            assert stepped[0].update_seq == 2
        finally:
            server.close()

    def test_publish_artifact_carries_update_seq_into_snapshots(self) -> None:
        from kith import ArtifactKey, Sim

        with Sim() as sim:
            key = ArtifactKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0)
            sim.publish(key, actor_id=9, pos=(1 << 16, 0, 0), input_tick=4, update_seq=7)
            artifacts = sim.snapshot_cell(key)
            assert len(artifacts) == 1
            assert artifacts[0].update_seq == 7
            assert artifacts[0].input_tick == 4


@needs_build
class TestTickHandler:
    def test_tick_handler_runs_once_per_tick_in_order(self) -> None:
        # The server advances a monotonic tick counter once per normal tick and
        # dispatches the Python callback to the worker pool as a single
        # task, so the reactor thread never enters the interpreter on the
        # tick path. The handler records each tick and requests shutdown after a
        # few invocations; the run loop drains and returns.
        server = Server()
        try:
            ticks: list[int] = []
            lock = threading.Lock()

            def on_tick(tick: int) -> None:
                with lock:
                    ticks.append(tick)
                    enough = len(ticks) >= 3
                if enough:
                    server.shutdown()

            server.register_tick_handler(on_tick)
            # Blocks until the handler requests shutdown and the reactor drains.
            server.run()
            with lock:
                recorded = list(ticks)
            assert len(recorded) >= 3
            assert recorded[0] == 1
            assert all(recorded[i] > recorded[i - 1] for i in range(1, len(recorded)))
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.close()

    def test_tick_handler_not_called_without_run(self) -> None:
        # Registering a tick callback without entering the run loop never
        # dispatches it; closing the facade clears the keep-alive slot.
        server = Server()
        called: list[int] = []
        server.register_tick_handler(lambda tick: called.append(tick))
        server.close()
        assert called == []


@needs_build
class TestFabricPublishSurface:
    # The composition root owns a fabric the gateway borrows; the facade
    # reaches it through the borrowed fabric accessor and renders a cell
    # product into the stream. A cell with no sim artifacts publishes a
    # zero-actor product header, so these cases need no sim setup.
    _KEY_ZONE: int = 1

    def test_publish_cell_product_returns_publish_seq(self) -> None:
        from kith import CellKey

        server = Server(topology="embedded")
        try:
            key = CellKey(zone=self._KEY_ZONE, cell_x=0, cell_y=0, cell_z=0, lod=0)
            seq = server.publish_cell_product(key, authority_epoch=1)
            # A fresh cell's first publish assigns a non-negative sequence;
            # re-publishing at the same epoch refreshes the product and
            # advances the sequence.
            again = server.publish_cell_product(key, authority_epoch=1)
            assert seq >= 0
            assert again >= 0
        finally:
            server.close()

    def test_publish_cell_product_stale_epoch_raises_eperm(self) -> None:
        from kith import CellKey, KithStateError

        server = Server(topology="embedded")
        try:
            key = CellKey(zone=self._KEY_ZONE, cell_x=0, cell_y=0, cell_z=0, lod=0)
            server.publish_cell_product(key, authority_epoch=10)
            # An epoch below the cell's current authority epoch is rejected:
            # the fabric keeps a monotonic epoch per cell so a stale replica
            # cannot overwrite a newer one.
            with pytest.raises(KithStateError) as exc_info:
                server.publish_cell_product(key, authority_epoch=9)
            assert exc_info.value.code == gen_types.kith_error.KITH_EPERM
        finally:
            server.close()


class TestSendSurface:
    # The composition-root mirror refuses a closed handle before touching
    # the session argument: no session can outlive the server, so the
    # closed check is the first thing send does.
    def test_send_after_close_raises_state_error(self) -> None:
        server = Server(topology="embedded")
        server.close()
        with pytest.raises(KithStateError) as exc_info:
            server.send(None, 1, b"")  # type: ignore[arg-type]
        assert exc_info.value.code == gen_types.kith_error.KITH_ESTATE


class TestBroadcastSurface:
    _KEY_ZONE = 77

    # The broadcast mirror refuses a closed handle before anything else.
    def test_broadcast_after_close_raises_state_error(self) -> None:
        from kith import CellKey

        server = Server(topology="embedded")
        server.close()
        key = CellKey(zone=self._KEY_ZONE, cell_x=0, cell_y=0, cell_z=0, lod=0)
        with pytest.raises(KithStateError) as exc_info:
            server.broadcast_cell(key, 1100, b"late line")
        assert exc_info.value.code == gen_types.kith_error.KITH_ESTATE

    # A payload past the negotiated maximum frame payload is refused at
    # submit: the queue never holds a request that cannot encode.
    def test_broadcast_oversize_raises_protocol_error(self) -> None:
        from kith import CellKey
        from kith.exceptions import KithProtocolError

        server = Server(topology="embedded")
        try:
            key = CellKey(zone=self._KEY_ZONE, cell_x=0, cell_y=0, cell_z=0, lod=0)
            with pytest.raises(KithProtocolError) as exc_info:
                server.broadcast_cell(key, 1100, bytes((1 << 20) + 1))
            assert exc_info.value.code == gen_types.kith_error.KITH_EPROTO
        finally:
            server.close()

    # A submit past the request queue's depth is refused with backpressure
    # and counted: with the run loop not ticking, sixty-four accepted
    # submits fill the queue and the sixty-fifth raises the transport
    # family. The header-only broadcast rides the same queue (an empty
    # payload is a legal frame), so the fill uses empty payloads too.
    def test_broadcast_saturation_raises_network_error(self) -> None:
        from kith import CellKey
        from kith.exceptions import KithNetworkError

        server = Server(topology="embedded")
        try:
            key = CellKey(zone=self._KEY_ZONE, cell_x=0, cell_y=0, cell_z=0, lod=0)
            for _ in range(64):
                server.broadcast_cell(key, 1100)
            with pytest.raises(KithNetworkError) as exc_info:
                server.broadcast_cell(key, 1100, b"one too many")
            assert exc_info.value.code == gen_types.kith_error.KITH_EAGAIN
        finally:
            server.close()


class TestLifecycleSurface:
    def test_session_count_after_close_raises_state_error(self) -> None:
        server = Server(topology="embedded")
        server.close()
        with pytest.raises(KithStateError) as exc_info:
            _ = server.session_count
        assert exc_info.value.code == gen_types.kith_error.KITH_ESTATE
