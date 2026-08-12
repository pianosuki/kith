"""Unit tests for the per-plane Python submodules and the Config wrapper.

The tests point ``KITH_LIB`` at the debug build directory so the ctypes bridge
loads the freshly built shared libraries, then reset the bridge singleton
around each test so cached state from a prior test cannot leak in. Each plane
module wraps its generated binding's C surface into Python types; the tests
exercise the lifecycle, the wrapped operations, and the error translation at
the boundary (the exception hierarchy in :mod:`kith.exceptions`).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from _build_gate import _BUILD_DEBUG, needs_build

from kith import (
    Aoi,
    Config,
    Coord,
    CoordBus,
    Fabric,
    KithConfigError,
    KithError,
    KithNotFoundError,
    KithProtocolError,
    KithStateError,
    Sim,
)
from kith._bridge import reset
from kith._generated import types as gen_types
from kith.aoi import Box, Object, Sphere
from kith.coord import RebalanceContract
from kith.fabric import CellKey, FabricArtifact, ProductLevel
from kith.proto import MsgFlag, Proto
from kith.sim import ArtifactKey


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


# ---------------------------------------------------------------------------
# exceptions
# ---------------------------------------------------------------------------


@needs_build
class TestExceptions:
    def test_hierarchy_families(self) -> None:
        assert issubclass(KithConfigError, KithError)
        assert issubclass(KithProtocolError, KithError)
        assert issubclass(KithStateError, KithError)
        assert issubclass(KithNotFoundError, KithError)

    def test_not_found_raised_for_enoent(self) -> None:
        with Proto() as proto:
            with pytest.raises(KithNotFoundError) as exc_info:
                proto.lookup_type("absent")
            assert exc_info.value.code == gen_types.kith_error.KITH_ENOENT
            assert isinstance(exc_info.value, KithError)

    def test_protocol_error_for_unknown_type_decode(self) -> None:
        with Proto() as proto, pytest.raises(KithProtocolError):
            # Encoding an unregistered type id succeeds (the codec does not
            # consult the registry on encode), but decode rejects frames
            # whose type_id is not registered.
            frame = proto.encode(9999, b"x")
            proto.decode(frame)


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------


@needs_build
class TestConfig:
    def test_builds_from_env_file(self, tmp_path: Path) -> None:
        env_file = tmp_path / "game.env"
        env_file.write_text("tick_hz = 30\nname = alpha\n", encoding="utf-8")
        with Config(file_path=env_file) as cfg:
            assert cfg.has("tick_hz")
            assert cfg.get_u32("tick_hz", default=20) == 30
            assert cfg.get_string("name") == "alpha"

    def test_missing_key_returns_default(self, tmp_path: Path) -> None:
        env_file = tmp_path / "game.env"
        env_file.write_text("present = 1\n", encoding="utf-8")
        with Config(file_path=env_file) as cfg:
            assert cfg.get_string("absent", default="fallback") == "fallback"
            assert cfg.get_bool("absent") is False
            assert cfg.get_u16("absent", default=7) == 7

    def test_bool_spellings(self, tmp_path: Path) -> None:
        env_file = tmp_path / "game.env"
        env_file.write_text("a = yes\nb = 0\nc = true\n", encoding="utf-8")
        with Config(file_path=env_file) as cfg:
            assert cfg.get_bool("a") is True
            assert cfg.get_bool("b") is False
            assert cfg.get_bool("c") is True

    def test_range_violation_raises_config_error(self, tmp_path: Path) -> None:
        env_file = tmp_path / "game.env"
        env_file.write_text("port = 99999\n", encoding="utf-8")
        with Config(file_path=env_file) as cfg:
            with pytest.raises(KithConfigError) as exc_info:
                cfg.get_u16("port", maximum=65535)
            assert exc_info.value.code == gen_types.kith_error.KITH_ERANGE

    def test_unreadable_file_raises_config_error(self, tmp_path: Path) -> None:
        with pytest.raises(KithConfigError) as exc_info:
            Config(file_path=tmp_path)  # a directory is not a readable file
        assert exc_info.value.code == gen_types.kith_error.KITH_EIO

    def test_env_prefix_overrides_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        env_file = tmp_path / "game.env"
        env_file.write_text("key = from_file\n", encoding="utf-8")
        monkeypatch.setenv("KITHTEST_key", "from_env")
        with Config(file_path=env_file, env_prefix="KITHTEST_") as cfg:
            assert cfg.get_string("key") == "from_env"


# ---------------------------------------------------------------------------
# proto
# ---------------------------------------------------------------------------


@needs_build
class TestProto:
    def test_register_lookup_roundtrip(self) -> None:
        with Proto() as proto:
            proto.register_type("move", 1100)
            assert proto.lookup_type("move") == 1100
            assert proto.type_name(1100) == "move"
            assert proto.type_name(9999) is None

    def test_encode_decode_roundtrip(self) -> None:
        with Proto() as proto:
            proto.register_type("move", 1100)
            frame = proto.encode(1100, b"hello")
            decoded = proto.decode(frame)
            assert decoded.type_id == 1100
            assert decoded.payload == b"hello"
            assert decoded.has_correlation is False

    def test_encode_decode_with_correlation(self) -> None:
        with Proto() as proto:
            proto.register_type("move", 1100)
            frame = proto.encode(
                1100, b"payload", flags=MsgFlag.CORRELATION, correlation_id=0xDEADBEEF
            )
            decoded = proto.decode(frame)
            assert decoded.has_correlation is True
            assert decoded.correlation_id == 0xDEADBEEF

    def test_correlation_hex_format(self) -> None:
        with Proto() as proto:
            assert proto.correlation_hex(0xDEADBEEF) == "00000000deadbeef"
            assert proto.correlation_hex(0) == "0000000000000000"

    def test_incomplete_frame_raises_protocol_error(self) -> None:
        with Proto() as proto, pytest.raises(KithProtocolError):
            proto.decode(b"\x00")  # far too short for a header


# ---------------------------------------------------------------------------
# AOI
# ---------------------------------------------------------------------------


@needs_build
class TestAoi:
    def test_insert_lookup_remove(self) -> None:
        with Aoi() as aoi:
            aoi.insert(Object(id=1, pos_x=10, pos_y=10, pos_z=0))
            aoi.insert(Object(id=2, pos_x=20, pos_y=20, pos_z=0))
            assert aoi.size() == 2
            found = aoi.lookup(1)
            assert found is not None and found.pos_x == 10
            assert aoi.lookup(99) is None
            aoi.remove(1)
            assert aoi.size() == 1
            assert aoi.lookup(1) is None

    def test_sphere_query_visits_intersecting(self) -> None:
        with Aoi() as aoi:
            aoi.insert(Object(id=1, pos_x=10, pos_y=10, pos_z=0))
            aoi.insert(Object(id=2, pos_x=100, pos_y=100, pos_z=0))
            visited: list[int] = []
            aoi.query_sphere(Sphere(cx=10, cy=10, cz=0, radius=5), _collect(visited))
            assert visited == [1]

    def test_box_query_visits_intersecting(self) -> None:
        with Aoi() as aoi:
            aoi.insert(Object(id=1, pos_x=10, pos_y=10, pos_z=0))
            aoi.insert(Object(id=2, pos_x=100, pos_y=100, pos_z=0))
            visited: list[int] = []
            aoi.query_box(Box(0, 0, 0, 1000, 1000, 0), _collect(visited))
            assert sorted(visited) == [1, 2]

    def test_update_moves_position(self) -> None:
        with Aoi() as aoi:
            aoi.insert(Object(id=1, pos_x=0, pos_y=0, pos_z=0))
            aoi.update(Object(id=1, pos_x=500, pos_y=500, pos_z=0))
            moved = aoi.lookup(1)
            assert moved is not None and moved.pos_x == 500


def _collect(into: list[int]) -> Callable[[Object], bool]:
    def _visit(obj: Object) -> bool:
        into.append(obj.id)
        return True

    return _visit


# ---------------------------------------------------------------------------
# sim + Fabric
# ---------------------------------------------------------------------------


@needs_build
class TestSimFabric:
    def test_publish_then_snapshot(self) -> None:
        with Sim() as sim:
            key = ArtifactKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0, authority_epoch=1)
            seq = sim.publish(key, actor_id=42, pos=(1, 2, 3), input_tick=1)
            assert seq >= 1
            assert sim.artifact_count() == 1
            arts = sim.snapshot_cell(key)
            assert len(arts) == 1
            assert arts[0].actor_id == 42
            assert arts[0].pos_x == 1
            product = sim.cell_product(key)
            assert product is not None
            assert product.actor_count == 1
            sim.remove_actor(42)
            assert sim.artifact_count() == 0
            assert sim.cell_product(key) is None

    def test_remove_zone_clears_cells(self) -> None:
        with Sim() as sim:
            key = ArtifactKey(zone=5, cell_x=0, cell_y=0, cell_z=0, lod=0, authority_epoch=1)
            sim.publish(key, actor_id=1, pos=(0, 0, 0))
            assert sim.artifact_count() == 1
            sim.remove_zone(5)
            assert sim.artifact_count() == 0

    def test_fabric_publish_and_subscription(self) -> None:
        with Sim() as sim, Fabric(sim) as fabric:
            key = CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0)
            fabric.publish(key, authority_epoch=1)
            assert fabric.product_count() == 1
            product = fabric.cell_product(key)
            assert product is not None
            assert product.authority_epoch == 1

            # Subscribe before publishing again so the change is captured.
            with fabric.create_subscription() as sub:
                sub.add(key)
                assert sub.size() == 1
                assert sub.drain() == []  # nothing changed since subscribing
                fabric.publish(key, authority_epoch=1)
                drained = sub.drain()
                assert len(drained) >= 1
                assert any(p.key == key for p in drained)
            assert sub.size() == 0

    def test_fabric_snapshot_zone_cells(self) -> None:
        with Sim() as sim, Fabric(sim) as fabric:
            key = CellKey(zone=2, cell_x=0, cell_y=0, cell_z=0, lod=0)
            fabric.publish(key, authority_epoch=1)
            products = fabric.snapshot_zone_cells(zone=2, lod=0)
            assert any(p.key == key for p in products)

    def test_fabric_snapshot_cell_returns_dataclasses(self) -> None:
        with Sim() as sim, Fabric(sim) as fabric:
            sim.publish(
                ArtifactKey(zone=3, cell_x=0, cell_y=0, cell_z=0, lod=0, authority_epoch=1),
                actor_id=13,
                pos=(4, 5, 6),
                input_tick=7,
            )
            key = CellKey(zone=3, cell_x=0, cell_y=0, cell_z=0, lod=0)
            fabric.publish(key, authority_epoch=1)
            arts = fabric.snapshot_cell(key, ProductLevel.FULL)
            assert len(arts) == 1
            art = arts[0]
            assert isinstance(art, FabricArtifact)
            assert art.actor_id == 13
            assert (art.pos_x, art.pos_y, art.pos_z) == (4, 5, 6)
            assert art.input_tick == 7
            assert art.product_level == ProductLevel.FULL

    def test_fabric_stale_authority_raises(self) -> None:
        with Sim() as sim, Fabric(sim) as fabric:
            key = CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0)
            fabric.publish(key, authority_epoch=2)
            with pytest.raises(KithError) as exc_info:
                fabric.publish(key, authority_epoch=1)  # stale epoch
            assert exc_info.value.code == gen_types.kith_error.KITH_EPERM


# ---------------------------------------------------------------------------
# coord + CoordBus
# ---------------------------------------------------------------------------


@needs_build
class TestCoord:
    def test_bus_publish_drain(self) -> None:
        with CoordBus(instance_id=1) as bus:
            assert bus.instance_id() == 1
            bus.publish(event_type=0, zone=7, payload=b"hello")
            events = bus.drain()
            assert len(events) == 1
            assert events[0].zone == 7
            assert events[0].event_type == 0
            assert events[0].payload == b"hello"
            assert bus.drain() == []

    def test_coord_authority_for_single_instance(self) -> None:
        with CoordBus(instance_id=3) as bus, Coord(bus, instance_id=3) as coord:
            auth = coord.authority(CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0))
            assert auth.instance_id == 3
            coord.set_authority(CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0), instance_id=9)
            auth = coord.authority(CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0))
            assert auth.instance_id == 9
            assert auth.authority_epoch >= 1
            coord.clear_authority(CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0))
            auth = coord.authority(CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0))
            assert auth.instance_id == 3

    def test_coord_without_bus_uses_local_instance(self) -> None:
        with Coord(instance_id=5) as coord:
            auth = coord.authority(CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0))
            assert auth.instance_id == 5
            assert coord.cell_count() >= 0

    def test_on_rebalance_applies_split_and_merge(self) -> None:
        # Two coords borrowing one shared bus: a split contract published on
        # the bus is drained and applied to the receiving coord, transferring
        # cell authority to the target instance; a merge contract (target 0)
        # clears the override. The contract round-trips through its byte
        # layout, matching the wire form a real bus event carries.
        key = CellKey(zone=1, cell_x=2, cell_y=3, cell_z=0, lod=0)
        with CoordBus(instance_id=1) as bus, Coord(bus, instance_id=2) as dst:
            bus.subscribe(1)  # zone 1

            split = RebalanceContract(
                key=key,
                source_instance_id=1,
                target_instance_id=2,
                authority_epoch=7,
            )
            bus.publish(
                event_type=0,  # KITH_COORD_BUS_EVENT_REBALANCE
                zone=1,
                payload=split.to_bytes(),
            )
            events = bus.drain()
            assert len(events) == 1
            applied = RebalanceContract.from_bytes(events[0].payload)
            assert applied == split
            dst.on_rebalance(applied)

            auth = dst.authority(key)
            assert auth.instance_id == 2
            assert auth.authority_epoch == 7

            merge = RebalanceContract(
                key=key,
                source_instance_id=2,
                target_instance_id=0,
                authority_epoch=8,
            )
            bus.publish(event_type=0, zone=1, payload=merge.to_bytes())
            events = bus.drain()
            assert len(events) == 1
            dst.on_rebalance(RebalanceContract.from_bytes(events[0].payload))

            auth = dst.authority(key)
            # Merge clears the override; the hash-fallback for a single-member
            # bus is the local instance (coord 2) with epoch 0.
            assert auth.instance_id == 2
            assert auth.authority_epoch == 0
            assert dst.cell_count() == 0

    def test_bus_add_member_validation(self) -> None:
        # The bus is created with one member (the local instance); add_member
        # extends the table. instance_id 0 is rejected (reserved for the
        # embedded/unowned sentinel) and a duplicate is rejected with
        # KithStateError, without growing the table.
        with CoordBus(instance_id=1) as bus:
            assert bus.member_count() == 1

            with pytest.raises(KithError):
                bus.add_member(0)

            bus.add_member(2)
            bus.add_member(7)
            assert bus.member_count() == 3
            assert bus.member_status(0).instance_id == 1
            assert bus.member_status(1).instance_id == 2
            assert bus.member_status(2).instance_id == 7

            with pytest.raises(KithStateError):
                bus.add_member(2)
            assert bus.member_count() == 3

    def test_density_driven_split_across_coords(self) -> None:
        # Two coords borrowing one loopback bus form a real multi-member
        # cluster once add_member extends the membership table. The
        # density-driven evaluator fires on the overloaded coord, picks the
        # least-loaded peer as the split target, and broadcasts a rebalance
        # contract; the peer drains and applies it, so both coords converge
        # on the new authority.
        key = CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0)
        with CoordBus(instance_id=2) as bus:
            bus.add_member(1)
            assert bus.member_count() == 2

            with (
                Coord(
                    bus,
                    instance_id=1,
                    split_threshold=10,
                    merge_threshold=5,
                    split_min_dwell_ms=100,
                    merge_min_dwell_ms=100,
                    density_stride=1,
                ) as a,
                Coord(
                    bus,
                    instance_id=2,
                    split_threshold=10,
                    merge_threshold=5,
                    split_min_dwell_ms=100,
                    merge_min_dwell_ms=100,
                    density_stride=1,
                ) as b,
            ):
                # Hash-fallback for this cell with members [2, 1]:
                # (0+0+1) % 2 = 1 -> members[1] = instance 1 (coord a).
                assert a.authority(key).instance_id == 1

                # Report density above the split threshold; advance past the
                # dwell. The report timestamp must be non-zero: report_density
                # arms split_since_ms only when it is zero, and tick treats a
                # zero split_since_ms as "not armed."
                a.report_density(key, actor_count=100, now_ms=1000)
                a.tick(now_ms=2000)

                # Coord a holds the split: the override points at instance 2.
                auth = a.authority(key)
                assert auth.instance_id == 2
                assert auth.authority_epoch == 1

                # Coord b drains the broadcast rebalance and applies it.
                events = bus.drain()
                assert len(events) == 1
                assert events[0].event_type == 0  # KITH_COORD_BUS_EVENT_REBALANCE
                contract = RebalanceContract.from_bytes(events[0].payload)
                assert contract.target_instance_id == 2
                assert contract.authority_epoch == 1
                b.on_rebalance(contract)

                assert b.authority(key).instance_id == 2
                assert b.authority(key).authority_epoch == 1

                # Density drops below the merge threshold; advance past the
                # merge dwell. Coord a clears its override and broadcasts a
                # target-0 contract; coord b applies it and reverts to
                # hash-fallback.
                a.report_density(key, actor_count=0, now_ms=3000)
                a.tick(now_ms=4000)

                assert a.authority(key).instance_id == 1
                assert a.authority(key).authority_epoch == 0

                events = bus.drain()
                assert len(events) == 1
                merge = RebalanceContract.from_bytes(events[0].payload)
                assert merge.target_instance_id == 0
                b.on_rebalance(merge)

                assert b.authority(key).instance_id == 1
                assert b.authority(key).authority_epoch == 0
