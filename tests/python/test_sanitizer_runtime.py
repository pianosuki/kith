"""Configured compiler ownership in sanitizer runtime selection."""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "sanitizer-runtime.sh"


def _compiler(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/usr/bin/env bash\n"
        'case "$1" in\n'
        '  --version) echo "fixture compiler"; exit "${VERSION_EXIT:-0}" ;;\n'
        '  -print-file-name=*) [ "${QUERY_EXIT:-0}" = 0 ] || exit "$QUERY_EXIT" ;;\n'
        "  *) exit 99 ;;\n"
        "esac\n"
        'case "$1" in\n'
        '  *-x86_64.so) printf "%s\\n" "$ARCH_RUNTIME" ;;\n'
        '  *) printf "%s\\n" "$GENERIC_RUNTIME" ;;\n'
        "esac\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _resolve(directory: Path, env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(SCRIPT), *args], cwd=directory, env=env, capture_output=True, text=True, check=False
    )


def test_runtime_uses_configured_compiler_and_records_identity(tmp_path: Path) -> None:
    build = tmp_path / "build = configured"
    build.mkdir()
    compiler = tmp_path / "tool chain = configured" / "clang"
    _compiler(compiler)
    other = tmp_path / "other"
    other.mkdir()
    wrong = other / "clang"
    wrong.write_text("#!/usr/bin/env bash\nexit 88\n", encoding="utf-8")
    wrong.chmod(0o755)
    env = dict(os.environ, PATH=f"{other}:{os.environ['PATH']}", CC=str(wrong))
    for kind in ("asan", "tsan"):
        for layout, cache_type in (("generic", "STRING"), ("arch", "FILEPATH")):
            runtime = tmp_path / f"{kind} = {layout}.so"
            runtime.write_bytes(b"fixture runtime\n")
            env.update(
                GENERIC_RUNTIME=str(runtime if layout == "generic" else tmp_path / "absent"),
                ARCH_RUNTIME=str(runtime),
            )
            (build / "CMakeCache.txt").write_text(
                f"# CMAKE_C_COMPILER:FILEPATH={wrong}\nCMAKE_C_COMPILER:{cache_type}={compiler}",
                encoding="utf-8",
            )
            result = _resolve(tmp_path, env, kind, str(build))
            assert result.returncode == 0, result.stderr
            assert result.stdout == f"{runtime}\n"
            assert "fixture compiler" in result.stderr
            for path in (compiler, runtime):
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                assert f"{digest}  {path}" in result.stderr


def test_runtime_rejects_incomplete_or_unsupported_build_identity(tmp_path: Path) -> None:
    build = tmp_path / "build"
    build.mkdir()
    compiler = tmp_path / "compiler"
    _compiler(compiler)
    nonexecutable = tmp_path / "nonexecutable"
    nonexecutable.write_text("not executable\n", encoding="utf-8")
    runtime = tmp_path / "runtime.so"
    runtime.write_bytes(b"fixture runtime\n")
    valid = f"CMAKE_C_COMPILER:STRING={compiler}\n"
    cases: list[tuple[str, list[str], str, dict[str, str]]] = [
        ("missing argument", ["asan"], valid, {}),
        ("extra argument", ["asan", str(build), "extra"], valid, {}),
        ("invalid kind", ["other", str(build)], valid, {}),
        ("missing cache", ["asan", str(build / "missing")], valid, {}),
        ("missing compiler", ["asan", str(build)], "", {}),
        ("empty compiler", ["asan", str(build)], "CMAKE_C_COMPILER:STRING=\n", {}),
        ("empty type", ["asan", str(build)], f"CMAKE_C_COMPILER:={compiler}\n", {}),
        ("duplicate", ["asan", str(build)], valid + valid, {}),
        ("relative compiler", ["asan", str(build)], "CMAKE_C_COMPILER:STRING=compiler\n", {}),
        ("missing compiler file", ["asan", str(build)], "CMAKE_C_COMPILER:FILEPATH=/absent\n", {}),
        ("compiler query failure", ["asan", str(build)], valid, {"QUERY_EXIT": "9"}),
        ("nonexecutable", ["asan", str(build)], f"CMAKE_C_COMPILER:FILEPATH={nonexecutable}\n", {}),
        (
            "compiler arguments",
            ["asan", str(build)],
            valid + "CMAKE_C_COMPILER_ARG1:STRING=-m32\n",
            {},
        ),
        (
            "compiler target",
            ["asan", str(build)],
            valid + "CMAKE_C_COMPILER_TARGET:STRING=other\n",
            {},
        ),
        (
            "missing runtime",
            ["asan", str(build)],
            valid,
            {"GENERIC_RUNTIME": "/absent", "ARCH_RUNTIME": "/absent"},
        ),
        (
            "relative runtime",
            ["asan", str(build)],
            valid,
            {"GENERIC_RUNTIME": "runtime.so", "ARCH_RUNTIME": "runtime.so"},
        ),
        ("version failure", ["asan", str(build)], valid, {"VERSION_EXIT": "9"}),
    ]
    for label, args, cache, overrides in cases:
        (build / "CMakeCache.txt").write_text(cache, encoding="utf-8")
        env = dict(os.environ, GENERIC_RUNTIME=str(runtime), ARCH_RUNTIME=str(runtime))
        env.update(overrides)
        result = _resolve(tmp_path, env, *args)
        assert result.returncode != 0, label
        assert result.stdout == "", label
        assert result.stderr, label
