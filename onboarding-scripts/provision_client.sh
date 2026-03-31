#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <tenant-id>"
  exit 1
fi

tenant="$1"
if [[ ! "${tenant}" =~ ^[a-z0-9-]{2,64}$ ]]; then
  echo "Invalid tenant-id. Use 2-64 chars: lowercase letters, digits, hyphen."
  exit 1
fi
name="mautic-${tenant}"

if docker ps -a --format '{{.Names}}' | grep -q "^${name}$"; then
  echo "Container ${name} already exists"
  exit 0
fi

docker run -d --name "${name}" --network app-net -p 0:80 \
  -e MAUTIC_TENANT="${tenant}" php:8.2-apache \
  sh -c "printf '%s\n' \"<?php \\$tenant=getenv('MAUTIC_TENANT'); if(!preg_match('/^[a-z0-9-]{2,64}$/', \\$tenant)){http_response_code(400); echo 'Invalid tenant'; exit;} echo 'Mautic instance for ' . htmlspecialchars(\\$tenant, ENT_QUOTES, 'UTF-8'); ?>\" > /var/www/html/index.php && apache2-foreground"

echo "Provisioned ${name}"
