#!/usr/bin/env bash
# NCAA D1 roster scrape — men's + women's, current season only.
#
# Invoked by Replit Scheduled Deployments; see
# docs/replit-scheduled-deployments.md for the console config.
#
# COACH_MISSES_REPORT_ENABLED=true tells the gated writer in
# scraper/extractors/ncaa_rosters.py (~line 2382) to record one row
# per school where the head coach could not be extracted from either
# the inline roster or the /coaches/staff fallback. Those rows feed
# the /data-quality/coach-misses dashboard. Only the scheduled run
# sets this so ad-hoc operator runs do not pollute the table.
#
# SCRAPE_TRIGGERED_BY=scheduler is read by scraper/scrape_run_logger.py
# and stamped onto every scrape_run_logs row so we can distinguish
# scheduled runs from operator-invoked runs.

set -euo pipefail

cd "$(dirname "$0")/.."

export SCRAPE_TRIGGERED_BY=scheduler
export COACH_MISSES_REPORT_ENABLED=true

python3 run.py --source ncaa-rosters --all --division D1 --gender mens
python3 run.py --source ncaa-rosters --all --division D1 --gender womens
