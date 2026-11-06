#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>

#include <unistd.h>

#include "kith/version.h"
#include "kith/worker/worker.h"

#include <sys/wait.h>

static void destroy_own_pool(void *ctx)
{
    kith_worker_destroy(ctx);
    _exit(3);
}

int main(void)
{
    int channel[2];
    if (pipe(channel) != 0)
    {
        return 1;
    }
    const pid_t child = fork();
    if (child < 0)
    {
        return 1;
    }
    if (child == 0)
    {
        (void)close(channel[0]);
        if (dup2(channel[1], STDERR_FILENO) < 0)
        {
            _exit(2);
        }
        (void)close(channel[1]);
        (void)alarm(5u);
        const kith_worker_params_t params = {
            .size = sizeof(params),
            .abi_version = KITH_ABI_VERSION,
            .worker_count = 2u,
            .task_capacity = 4u,
        };
        kith_worker_t *pool = nullptr;
        if (kith_worker_create(&params, nullptr, &pool) != 0 ||
            kith_worker_submit(pool, destroy_own_pool, pool) != 0)
        {
            _exit(2);
        }
        for (;;)
        {
            (void)pause();
        }
    }
    (void)close(channel[1]);
    char output[4096] = {0};
    size_t used = 0u;
    char buffer[512];
    for (;;)
    {
        const ssize_t count = read(channel[0], buffer, sizeof(buffer));
        if (count < 0 && errno == EINTR)
        {
            continue;
        }
        if (count <= 0)
        {
            break;
        }
        const size_t remaining = sizeof(output) - 1u - used;
        const size_t copied = (size_t)count < remaining ? (size_t)count : remaining;
        memcpy(output + used, buffer, copied);
        used += copied;
    }
    (void)close(channel[0]);
    int status = 0;
    if (waitpid(child, &status, 0) != child || !WIFSIGNALED(status) ||
        WTERMSIG(status) != SIGABRT ||
        strstr(output, "kith_worker_destroy: cannot destroy a pool from its own task") == nullptr)
    {
        (void)fprintf(stderr, "worker self-destroy: status %d, stderr: %s\n", status, output);
        return 1;
    }
    return 0;
}
