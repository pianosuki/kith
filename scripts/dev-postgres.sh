#!/usr/bin/env bash
#
# Start a local CI-shaped Postgres for development.
#
# Provisions the same image, credentials, and health check the CI service
# containers use (.github/workflows/ci.yml) so the db-backed integration
# tests and examples behave locally as they do in CI:
#
#   image     postgres:16
#   role      kith / password kith
#   database  kith_example
#   health    pg_isready -U kith -d kith_example
#
# The container is named kith-dev-postgres and survives reboots; data
# persists across runs and re-running this script starts the existing
# container rather than recreating it. The game_state table the postgres
# example persists into is created idempotently once the server is
# healthy; the integration tests create their own schema through the
# framework's query transport.
#
# When the host already runs Postgres on 5432, pick another port first:
#
#   KITH_DEV_PG_PORT=55432 scripts/dev-postgres.sh
#
# On success the exact KITH_PG_* exports are printed; paste them into the
# shell before running the tests or the example. A SIGKILL'd run can leave
# the container behind; remove it with `docker rm -f kith-dev-postgres`.

set -euo pipefail

image="postgres:16"
name="kith-dev-postgres"
requested_port="${KITH_DEV_PG_PORT:-5432}"

if docker inspect "$name" > /dev/null 2>&1; then
    echo "==> dev-postgres.sh: starting existing container $name"
    docker start "$name" > /dev/null
else
    echo "==> dev-postgres.sh: pulling $image if needed, creating $name"
    if ! docker run -d --name "$name" \
        -p "127.0.0.1:${requested_port}:5432" \
        -e POSTGRES_USER=kith \
        -e POSTGRES_PASSWORD=kith \
        -e POSTGRES_DB=kith_example \
        --health-cmd "pg_isready -U kith -d kith_example" \
        --health-interval 10s \
        --health-timeout 5s \
        --health-retries 5 \
        "$image" > /dev/null
    then
        echo "==> dev-postgres.sh: could not start $name on 127.0.0.1:${requested_port}" >&2
        echo "    the port is likely taken by another service;" >&2
        echo "    retry with: KITH_DEV_PG_PORT=<port> $0" >&2
        docker rm -f "$name" > /dev/null 2>&1 || true
        exit 1
    fi
fi

printf '==> waiting for %s to become healthy' "$name"
healthy=""
for _ in $(seq 1 30); do
    status="$(docker inspect --format '{{.State.Health.Status}}' "$name")"
    if [ "$status" = "healthy" ]; then
        healthy=1
        break
    fi
    printf '.'
    sleep 1
done
echo
if [ -z "$healthy" ]; then
    echo "==> $name did not become healthy within 30s; last log lines:" >&2
    docker logs --tail 20 "$name" >&2 || true
    exit 1
fi

# docker port emits one line per binding; read it fully (a pipe to head
# would SIGPIPE-kill the writer under pipefail) and take the first port.
mappings="$(docker port "$name" 5432/tcp 2> /dev/null || true)"
host_port="${mappings%%$'\n'*}"
host_port="${host_port##*:}"
if [ -z "$host_port" ]; then
    host_port="$requested_port"
fi

docker exec "$name" psql -v ON_ERROR_STOP=1 -U kith -d kith_example \
    -c 'CREATE TABLE IF NOT EXISTS game_state (
            key text PRIMARY KEY,
            value text NOT NULL)' > /dev/null

cat <<EOF

Postgres ready on 127.0.0.1:${host_port}. Export these before running the
db integration tests or the postgres example (they are required: without
them the client falls back to an empty password and authentication fails):

export KITH_PG_HOST=127.0.0.1
export KITH_PG_PORT=${host_port}
export KITH_PG_USER=kith
export KITH_PG_PASSWORD=kith
export KITH_PG_DB=kith_example
EOF
