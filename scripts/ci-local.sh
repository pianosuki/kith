#!/usr/bin/env bash
#
# Reproduce the push-gated GitHub Actions workflows locally via Docker.
#
# Builds the thin ubuntu-24.04 image (ci/ubuntu-24.04/Dockerfile) and runs
# each workflow job in its own container, shaped like the runner: the
# container creates a non-root user with the host's uid/gid and passwordless
# sudo — the runner account's shape, required because root's DAC override
# makes permission-dependent tests pass that must fail — then runs that
# job's provisioning mode followed by the job's own steps from
# .github/workflows/. The runner's postgres:16 service container is
# reproduced alongside on a private network for the jobs that attach one.
# If every job passes, CI passes.
#
# Jobs (default: all, in workflow order):
#   lint           ci.yml lint          — provision lint, then verify.sh lint
#   build          ci.yml build         — provision build, then verify.sh build
#   free-threaded  ci.yml free-threaded — provision free-threaded, the job's
#                                         cmake configure/build, then
#                                         verify.sh free-threaded
#   commits        ci.yml commits       — verify.sh commits
#   pages          pages.yml build      — apt install doxygen, then the
#                                         reference generation step
#
# Documented deltas from the runner:
#   - No actions/cache: hook environments, pip wheels, and the
#     free-threaded interpreter are re-fetched per container.
#   - actions/checkout is replaced by the bind mount, so the two build
#     trees land in build/ci and build/ci-ft (KITH_BUILD_DIR / KITH_LIB
#     follow) instead of the workflow's build/debug — CMake refuses to
#     reconfigure a cache whose source root differs from the developer's
#     own build/debug. The relocation is the only configure difference.
#   - The commits job's event-derived push range falls back to
#     verify.sh's origin/main fork-point resolution.
#   - The pages artifact upload and Pages deploy are runner-side
#     publishing; only the reference generation is reproduced.
#   - Scheduled and tag-gated workflows (sanitizers, coverage, stress,
#     release) gate no push; they are not emulated.
#
# The host user must be non-root for the same reason the container user is.

set -euo pipefail

image="kith-ci"
dockerfile="ci/ubuntu-24.04/Dockerfile"

cd "$(dirname "$0")/.."

if [ "$(id -u)" -eq 0 ]; then
    echo "ci-local.sh: run from a non-root user; root cannot reproduce the runner's DAC behavior" >&2
    exit 1
fi

jobs=("$@")
if [ "${#jobs[@]}" -eq 0 ]; then
    jobs=(lint build free-threaded commits pages)
fi
for job in "${jobs[@]}"; do
    case "$job" in
        lint|build|free-threaded|commits|pages) ;;
        *) echo "ci-local.sh: unknown job: $job" >&2; exit 1 ;;
    esac
done

echo "==> ci-local.sh: building image $image"
docker build -t "$image" -f "$dockerfile" "$(dirname "$dockerfile")"

# --- Postgres service ----------------------------------------------------
# ci.yml attaches a postgres:16 service container to the build and
# free-threaded jobs. Reproduce it on a per-invocation bridge network so
# those containers reach it by the same service-name resolution CI uses; no
# host port is published. Names are PID-suffixed so concurrent invocations
# cannot collide; a SIGKILL'd run can still leave them behind (docker
# system prune reclaims them).
net="kith-ci-local-net-$$"
pg="kith-ci-local-pg-$$"
job_tmp="$(mktemp -d -t kith-ci-local-XXXXXX)"
cleanup() {
    docker rm -f "$pg" > /dev/null 2>&1 || true
    docker network rm "$net" > /dev/null 2>&1 || true
    rm -rf "$job_tmp"
}
trap cleanup EXIT

need_pg=false
for job in "${jobs[@]}"; do
    case "$job" in
        build|free-threaded) need_pg=true ;;
    esac
done

if [ "$need_pg" = true ]; then
    docker network create "$net" > /dev/null
    echo "==> ci-local.sh: starting postgres service (first run pulls postgres:16)"
    docker run -d --name "$pg" --network "$net" --network-alias postgres \
        -e POSTGRES_USER=kith \
        -e POSTGRES_PASSWORD=kith \
        -e POSTGRES_DB=kith_example \
        --health-cmd "pg_isready -U kith -d kith_example" \
        --health-interval 10s \
        --health-timeout 5s \
        --health-retries 5 \
        postgres:16 > /dev/null

    printf '==> ci-local.sh: waiting for the postgres service to become healthy'
    pg_healthy=""
    for _ in $(seq 1 60); do
        status="$(docker inspect --format '{{.State.Health.Status}}' "$pg" 2> /dev/null || true)"
        if [ "$status" = "healthy" ]; then
            pg_healthy=1
            break
        fi
        printf '.'
        sleep 1
    done
    echo
    if [ -z "$pg_healthy" ]; then
        echo "==> ci-local.sh: postgres service did not become healthy; last log lines:" >&2
        docker logs --tail 20 "$pg" >&2 || true
        exit 1
    fi
fi

# --- Container entrypoint -------------------------------------------------
# The container starts as root only to set up the runner account: a user
# with the host's uid/gid owns the bind mount natively, so no artifact
# chown-back is needed, and passwordless sudo mirrors the runner account
# the provisioning script expects. ubuntu:24.04 ships an ubuntu user/group
# at id 1000; drop it when the host ids collide. The build-tree cleanup
# reproduces the fresh-runner state each job starts from. GITHUB_PATH and
# GITHUB_ENV point at runner-shaped files the provisioning script appends
# to; the job shell sources them at the provision/steps boundary exactly
# as the runner applies them between steps.
cat > "$job_tmp/entry.sh" <<'EOF'
set -e
if [ "$HOST_UID" = 1000 ] || [ "$HOST_GID" = 1000 ]; then
    userdel -r ubuntu 2>/dev/null || true
    groupdel ubuntu 2>/dev/null || true
fi
groupadd -g "$HOST_GID" ci
useradd -m -u "$HOST_UID" -g "$HOST_GID" -s /bin/bash ci
echo 'ci ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/ci
chmod 440 /etc/sudoers.d/ci
rm -rf build/ci build/ci-ft .venv-ft .pytest_cache build/doxygen
export GITHUB_PATH=/tmp/kith-gh-path GITHUB_ENV=/tmp/kith-gh-env
runuser -u ci -- bash -ec ': > "$GITHUB_PATH" && : > "$GITHUB_ENV"'
runuser -u ci -- bash -e /job/job.sh
EOF

# Sourced by provision-carrying jobs between the provisioning script and
# the job's own steps: applies what the script wrote to the runner env
# files (the ~/.local/bin tools, LIBCLANG_LIBRARY_FILE) to this shell.
cat > "$job_tmp/apply-env.sh" <<'EOF'
if [ -f "$GITHUB_PATH" ]; then
    while IFS= read -r p; do
        [ -n "$p" ] && PATH="$p:$PATH"
    done < "$GITHUB_PATH"
    export PATH
fi
if [ -f "$GITHUB_ENV" ]; then
    while IFS= read -r kv; do
        case "$kv" in [A-Za-z_]*=*) export "$kv" ;; esac
    done < "$GITHUB_ENV"
fi
EOF

# --- Job definitions ------------------------------------------------------
# Each value is the job's shell, executed as the non-root ci user from the
# repository root. provision-ci.sh <mode> precedes the workflow's own steps,
# exactly as the job definition orders them; the two build-tree relocations
# are the documented deltas above.
job_scripts=(
    "lint|./scripts/provision-ci.sh lint
. /job/apply-env.sh
./scripts/verify.sh lint"
    "build|./scripts/provision-ci.sh build
. /job/apply-env.sh
KITH_C_COMPILER=clang KITH_BUILD_DIR=build/ci ./scripts/verify.sh build"
    "free-threaded|./scripts/provision-ci.sh free-threaded
. /job/apply-env.sh
cmake --preset debug -B build/ci-ft
cmake --build build/ci-ft
KITH_BUILD_DIR=build/ci-ft ./scripts/verify.sh free-threaded"
    "commits|./scripts/verify.sh commits"
    "pages|sudo apt-get update
sudo apt-get install -y --no-install-recommends doxygen
mkdir -p build/doxygen
doxygen Doxyfile"
)

for wanted in "${jobs[@]}"; do
    for entry in "${job_scripts[@]}"; do
        name="${entry%%|*}"
        [ "$name" = "$wanted" ] || continue
        script="${entry#*|}"
        printf '%s\n' "$script" > "$job_tmp/job.sh"

        net_args=()
        pg_args=()
        if [ "$need_pg" = true ]; then
            net_args=(--network "$net")
            if [ "$name" = "build" ] || [ "$name" = "free-threaded" ]; then
                pg_args=(
                    -e KITH_PG_HOST=postgres
                    -e KITH_PG_PORT=5432
                    -e KITH_PG_USER=kith
                    -e KITH_PG_PASSWORD=kith
                    -e KITH_PG_DB=kith_example
                )
            fi
        fi

        echo "==> ci-local.sh: job $name (provision + steps, non-root runner shape)"
        set +e
        docker run --rm \
            "${net_args[@]}" \
            --security-opt seccomp=unconfined \
            -e HOST_UID="$(id -u)" \
            -e HOST_GID="$(id -g)" \
            -e CI=true \
            "${pg_args[@]}" \
            -v "$PWD":/work -w /work \
            -v "$job_tmp":/job:ro \
            "$image" \
            bash /job/entry.sh
        rc=$?
        set -e
        if [ "$rc" -ne 0 ]; then
            echo "==> ci-local.sh: job $name FAILED (exit $rc); CI would be red" >&2
            exit "$rc"
        fi
    done
done

echo "==> ci-local.sh: all jobs passed; CI stays green"
