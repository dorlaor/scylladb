#!/bin/bash
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Run the scylla-fd-guard test suite (test_fd_guard.py) inside a container.
#
# Without extra arguments the container is unprivileged, so the tests that
# raise a *hard* RLIMIT_NOFILE (which needs CAP_SYS_RESOURCE in the initial
# user namespace) are skipped. To also run those, give the container real
# root privileges, e.g.:
#
#   sudo docker: test_fd_guard_container.sh --privileged
#   krun micro-VM (works rootless, needs crun-krun + libkrun installed):
#                 test_fd_guard_container.sh --runtime /usr/bin/krun
#
# Any arguments are passed through to `<engine> run`.

set -e

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"

engine="${CONTAINER_ENGINE:-$(command -v podman || command -v docker)}"
image="${IMAGE:-docker.io/library/python:3.12-slim}"

exec "$engine" run --rm "$@" \
    -v "$repo/dist/common/scripts/scylla_fd_guard:/src/dist/common/scripts/scylla_fd_guard:ro" \
    -v "$here/test_fd_guard.py:/src/test/dist_test/test_fd_guard.py:ro" \
    -e PYTHONDONTWRITEBYTECODE=1 \
    "$image" \
    bash -c 'pip -q install pytest && pytest -p no:cacheprovider -v /src/test/dist_test/test_fd_guard.py'
