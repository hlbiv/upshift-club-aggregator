"""
Hand-picked seed list for the WordPress tryouts scraper.

Each entry is a best-effort guess at a club that probably runs a
WordPress site with tryout info under one of the paths probed by
``scrape_tryouts_wordpress``. The runner never fails on a miss — sites
without a matching tryout page are silently skipped, so the cost of a
wrong guess is one HTTP request.

Grow this list over time. When a scheduled run produces zero rows for
an entry for several seasons, remove it.

To add a site:
    1. Visit the club's public website.
    2. Confirm it's WordPress (usually ``/wp-content/`` in the asset URLs).
    3. Check that ``/tryouts/`` or ``/register/`` returns a page with a
       date string in recognizable format (see `parse_date`).
    4. Add a ``{"club_name_raw", "website"}`` entry below and rerun
       ``python scraper/run.py --source tryouts-wordpress --dry-run``.

NOTE: since I don't have live DB access from this build context, the
list starts empty. The runner tolerates an empty seed — it simply does
nothing. Operator backfills this list from
``canonical_clubs WHERE staff_page_url IS NOT NULL`` on Replit after
the first real run.
"""

from __future__ import annotations

from typing import Dict, List


TRYOUTS_WORDPRESS_SEED: List[Dict[str, str]] = [
    # TODO: backfill from canonical_clubs on Replit. Example shape:
    # {"club_name_raw": "Atlanta Fire United", "website": "https://atlantafireunited.com"},
]
