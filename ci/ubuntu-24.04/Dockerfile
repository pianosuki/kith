# Local CI emulation image.
#
# Mirrors the ubuntu-24.04 GitHub Actions runner: the image carries only the
# runner's base packages, and every push-gated workflow job is reproduced
# per job — provisioning included — by scripts/ci-local.sh. Each job's
# container drops to a non-root user (the runner's shape, with passwordless
# sudo) and runs scripts/provision-ci.sh <mode> followed by that job's own
# steps from .github/workflows/. Provisioning at image build time would
# union every job's dependencies and mask exactly the per-job gaps this
# emulation exists to catch.
#
# The image itself is intentionally thin: nothing from the repository is
# copied in. The build context is this Dockerfile's directory:
#
#   docker build -t kith-ci -f ci/ubuntu-24.04/Dockerfile ci/ubuntu-24.04
#   scripts/ci-local.sh            # reproduce every push-gated job
#
# If a job passes here, it passes on the runner.

# The digest pins the image bytes; the tag documents the intended version.
# The pin also freezes the apt package snapshot the provisioning script sees,
# so bumping is deliberate: re-resolve the tag's digest and update both.
FROM ubuntu:24.04@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3

# Avoid tzdata and other interactive prompts during apt installs.
ENV DEBIAN_FRONTEND=noninteractive

# The runner base: wget fetches the LLVM apt key, gnupg dearmors it, sudo
# backs the provisioning script's privileged wrapper, git backs the commits
# stage and the git ls-files checkers, python3/pip back the pip --user tool
# installs, and make backs the out-of-tree consumer stage in verify.sh and
# the liburing source build.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates wget gnupg sudo git python3 python3-pip make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work
