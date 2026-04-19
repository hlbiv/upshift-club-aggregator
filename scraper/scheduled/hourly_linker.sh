#!/usr/bin/env bash
# Hourly canonical-club linker run.
#
# Resolves `team_name_raw` / `home_team_name` / `away_team_name` text
# columns into `canonical_club_id` / `home_club_id` / `away_club_id`
# FKs on event_teams / matches / club_roster_snapshots / roster_diffs /
# tryouts. See CLAUDE.md "Canonical-Club Linker" for the 4-pass
# resolver, and docs/replit-scheduled-deployments.md for the console
# config. The SCRAPE_TRIGGERED_BY env var is read by
# scraper/scrape_run_logger.py and stamped onto scrape_run_logs.

set -euo pipefail

cd "$(dirname "$0")/.."

SCRAPE_TRIGGERED_BY=scheduler python3 run.py --source link-canonical-clubs
