#!/bin/sh
set -eu

cd /home/container

STARTUP=${STARTUP:-"/mcxboxbroadcast -config /home/container/config.yml"}
MODIFIED_STARTUP=$(eval echo "$(printf '%s' "$STARTUP" | sed -e 's/{{/${/g' -e 's/}}/}/g')")

echo ":/home/container$ ${MODIFIED_STARTUP}"
exec sh -c "${MODIFIED_STARTUP}"
