/*
 * Minimal C boot for the kith composition root.
 *
 * Builds a server handle from a NULL config (every plane wired with its
 * defaults, an OS-assigned ephemeral gateway port, embedded topology),
 * prints the bound port, installs a SIGINT handler that latches the
 * signal, and enters the run loop. A dedicated watcher thread polls the
 * latch and calls kith_server_shutdown (which is thread-safe but not
 * async-signal-safe); the run loop blocks until the next tick observes
 * the shutdown flag, then drains and the handle is released.
 *
 * No wire types, handlers, models, zones, queries, or control routes are
 * registered: this is the thinnest end-to-end slice of the C surface, the
 * counterpart to examples/minimal/server.py. It proves the composition root
 * builds, runs, and tears down standalone.
 */

#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#include <pthread.h>

#include "kith/server/server.h"
#include "kith/types.h"
#include "kith/version.h"

// Latched by the SIGINT handler; the watcher thread reads this and calls
// kith_server_shutdown. atomic_int stores are async-signal-safe.
static atomic_int g_sigint_latch = 0;

static void on_sigint(int sig)
{
    (void)sig;
    atomic_store(&g_sigint_latch, 1);
}

// Watcher thread: poll the latch at a short interval and, when it trips,
// request graceful shutdown. kith_server_shutdown is thread-safe (it
// performs atomic stores only); it is not async-signal-safe, so it is
// called here, not from the signal handler.
static void *watch_shutdown(void *arg)
{
    kith_server_t *server = (kith_server_t *)arg;
    for (;;)
    {
        if (atomic_load(&g_sigint_latch) != 0)
        {
            (void)kith_server_shutdown(server);
            return nullptr;
        }
        struct timespec ts = {.tv_sec = 0, .tv_nsec = 5L * 1000 * 1000};
        (void)nanosleep(&ts, nullptr);
    }
}

int main(void)
{
    kith_server_t *server = nullptr;
    int rc = kith_server_create(nullptr, nullptr, &server);
    if (rc != 0)
    {
        (void)fprintf(stderr, "minimal: kith_server_create failed: %d\n", rc);
        return 1;
    }

    (void)printf("minimal: gateway=%u\n", (unsigned)kith_server_listen_port(server));
    (void)fflush(stdout);

    (void)signal(SIGINT, on_sigint);

    pthread_t watcher;
    int prc = pthread_create(&watcher, nullptr, watch_shutdown, server);
    if (prc != 0)
    {
        kith_server_destroy(server);
        (void)fprintf(stderr, "minimal: pthread_create failed: %d\n", prc);
        return 1;
    }

    rc = kith_server_run(server);

    // The watcher loop returns once it has requested shutdown; join it so
    // no thread is left running past kith_server_destroy.
    atomic_store(&g_sigint_latch, 1);
    (void)pthread_join(watcher, nullptr);

    kith_server_destroy(server);

    if (rc != 0)
    {
        (void)fprintf(stderr, "minimal: kith_server_run failed: %d\n", rc);
        return 1;
    }
    return 0;
}
