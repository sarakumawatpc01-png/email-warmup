#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <tenant-id>"
  exit 1
fi

tenant="$1"
name="mautic-${tenant}"

if docker ps -a --format '{{.Names}}' | grep -q "^${name}$"; then
  echo "Container ${name} already exists"
  exit 0
fi

docker run -d --name "${name}" --network email-warmup_app-net -p 0:80 php:8.2-apache \
  sh -c "echo '<?php echo \"Mautic instance for ${tenant}\"; ?>' > /var/www/html/index.php && apache2-foreground"

echo "Provisioned ${name}"
