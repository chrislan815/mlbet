#!/usr/bin/env bash
# Apply manual host-side fixes to the mlb-db GCE VM.
# Idempotent — safe to re-run. Run from a machine with SSH access (alias: mlb-db) and gcloud auth.
#
# What this does:
#   1. Restricts the GCP `allow-postgres` firewall rule to a specific IP allowlist.
#   2. Lowers Postgres logrotate maxsize from 500M to 100M.
#   3. Adds an hourly logrotate cron for Postgres so the maxsize cap is actually enforced.
#
# Background: with port 5432 open to 0.0.0.0/0, scanner bots filled
# /var/log/postgresql/postgresql-15-main.log at ~160 MB/hr and crashed Postgres twice.

set -euo pipefail

# --- Edit this allowlist as IPs change ---
ALLOWLIST="47.155.29.203/32,23.241.167.201/32,146.75.146.170/32"
# 47.155.29.203 = teammate (Starlink, rotates — update as needed)
# 23.241.167.201 = Chris TCP egress
# 146.75.146.170 = Chris anycast

SSH_HOST="mlb-db"

echo "==> [1/3] Restricting allow-postgres firewall to: $ALLOWLIST"
gcloud compute firewall-rules update allow-postgres --source-ranges="$ALLOWLIST"

echo "==> [2/3] Lowering logrotate maxsize 500M -> 100M on $SSH_HOST"
ssh "$SSH_HOST" "sudo sed -i 's/maxsize 500M/maxsize 100M/' /etc/logrotate.d/postgresql-common"

echo "==> [3/3] Installing hourly Postgres logrotate cron on $SSH_HOST"
ssh "$SSH_HOST" "sudo tee /etc/cron.hourly/logrotate-postgres >/dev/null <<'EOF'
#!/bin/sh
/usr/sbin/logrotate /etc/logrotate.d/postgresql-common --state /var/lib/logrotate/postgres-status
EOF
sudo chmod +x /etc/cron.hourly/logrotate-postgres"

echo "==> Done."
