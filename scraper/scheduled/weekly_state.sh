#!/usr/bin/env bash
# Weekly state-association scrape — 54 USYS state associations.
#
# Invoked by Replit Scheduled Deployments; see
# docs/replit-scheduled-deployments.md for the console config.
# The SCRAPE_TRIGGERED_BY env var is read by scraper/scrape_run_logger.py
# and stamped onto every scrape_run_logs row so we can distinguish
# scheduled runs from operator-invoked runs.

set -euo pipefail

cd "$(dirname "$0")/.."

SCRAPE_TRIGGERED_BY=scheduler python3 run.py --scope state
