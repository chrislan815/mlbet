# Deploy Notes

Manual host-side config for the `mlb-db` GCE VM (the host running Postgres + the gameday FastAPI app at http://34.182.14.4:8080). These steps are **not** part of the Docker image — they live on the host and need to be reapplied if the VM is rebuilt.

## When to run

- After a fresh VM build / restore from snapshot.
- When the teammate's Starlink IP rotates and they get locked out (edit `ALLOWLIST` in the script first, then re-run).

## Prerequisites

- `gcloud` authenticated to project `avid-pager-393205`.
- SSH access to the VM via the `mlb-db` host alias (configured in `~/.ssh/config` with IAP tunnel).

## Run

```bash
./deploy/mlb-db-setup.sh
```

The script is idempotent. It:

1. Restricts the GCP `allow-postgres` firewall rule to a specific IP allowlist (defined at the top of the script).
2. Lowers `/etc/logrotate.d/postgresql-common` `maxsize` from 500M → 100M.
3. Installs `/etc/cron.hourly/logrotate-postgres` so the size cap is checked every hour, not just daily.

## Why this exists

Port 5432 was originally exposed to `0.0.0.0/0`. Scanner bots flooded auth attempts and the Postgres log grew at ~160 MB/hr. Daily logrotate couldn't keep up — the file ballooned to 27 GB and filled the disk twice, taking down both Postgres and the gameday API.

The firewall change kills the spam (~6 orders of magnitude reduction in log growth). The logrotate changes are a safety net for any future spike.

## Service layout on mlb-db

- `gameday.service` (systemd) — uvicorn on `0.0.0.0:8080`, working dir `/home/chrislan/mlb`, logs to `/home/chrislan/mlb/gameday.log`.
- `postgresql@15-main` — listens on `0.0.0.0:5432` (firewall-restricted), data in `/var/lib/postgresql/15/main`.
