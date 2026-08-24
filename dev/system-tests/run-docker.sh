#!/bin/bash
#
# Build the Debian package from this checkout, install it in a clean container,
# and run check-install.sh against it.
#
#   ./dev/system-tests/run-docker.sh [image]
#
# Default image is debian:12. Anything apt-based that has a matching python3
# should work.
#
# This needs docker and about two minutes. It leaves nothing behind: both
# containers are removed on the way out.
#
set -e

IMAGE=${1:-debian:12}
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
BUILDER=weewx-syscheck-build
TARGET=weewx-syscheck-run

cleanup() { docker rm -f "$BUILDER" "$TARGET" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

echo "== building the package in $IMAGE =="
docker run -d --name "$BUILDER" "$IMAGE" sleep infinity >/dev/null
docker exec "$BUILDER" bash -c '
    apt-get update -qq >/dev/null
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
        build-essential debhelper devscripts python3-all rsync >/dev/null 2>&1
    mkdir -p /src'
# git archive rather than a bind mount: it gives the builder exactly what is
# committed, with no build products and no local checkout quirks.
(cd "$REPO" && git archive --format=tar HEAD) | docker exec -i "$BUILDER" bash -c 'tar x -C /src'
docker exec "$BUILDER" bash -c 'cd /src && make debian-package >/dev/null 2>&1'
DEB=$(docker exec "$BUILDER" bash -c 'ls /src/dist/*.deb | head -1')
echo "   $(basename "$DEB")"

echo "== installing it in a clean $IMAGE =="
docker run -d --name "$TARGET" "$IMAGE" sleep infinity >/dev/null
# Stream the files in rather than using docker cp, which would need a host path
# and is awkward where the host is not the same kind of system as the container.
docker exec "$BUILDER" bash -c "cat $DEB"     | docker exec -i "$TARGET" bash -c 'cat > /tmp/weewx.deb'
docker exec -i "$TARGET" bash -c 'cat > /tmp/pmon.tgz' < "$REPO/src/weecfg/tests/pmon.tgz"
docker exec -i "$TARGET" bash -c 'cat > /tmp/check-install.sh' < "$HERE/check-install.sh"
docker exec "$TARGET" bash -c '
    apt-get update -qq >/dev/null 2>&1
    # rsyslog so that /dev/log exists; without it python logs a traceback per message
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq rsyslog >/dev/null 2>&1
    rsyslogd 2>/dev/null || true
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq /tmp/weewx.deb >/dev/null 2>&1'

echo "== checks =="
docker exec "$TARGET" bash -c 'bash /tmp/check-install.sh'
