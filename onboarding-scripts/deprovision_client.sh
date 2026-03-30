#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <tenant-id>"
  exit 1
fi

tenant="$1"
name="mautic-${tenant}"

if ! docker ps -a --format '{{.Names}}' | grep -q "^${name}$"; then
  echo "Container ${name} does not exist"
  exit 0
fi

docker stop "${name}" >/dev/null || true
docker rm "${name}" >/dev/null || true

echo "Deprovisioned ${name}"
