"""
Freshness SLA thresholds per entity type.

Used by the /analytics/scrape-health endpoint to flag stale domains
and by the reconciler to demote entities to 'stale' status.

Values are in hours. An entity is "stale" if last_scraped_at is older
than its SLA threshold.
"""

# entity_type -> max acceptable age in hours
FRESHNESS_SLA_HOURS: dict[str, int] = {
    "club": 168,       # 7 days -- club discovery runs weekly
    "league": 720,     # 30 days -- reference data, rarely changes
    "college": 168,    # 7 days -- roster + coach scrape weekly
    "coach": 168,      # 7 days -- staff scrape weekly
    "event": 24,       # 1 day -- events change frequently
    "match": 24,       # 1 day -- match results update daily during season
    "tryout": 168,     # 7 days -- tryout listings refresh weekly
}

DEFAULT_SLA_HOURS = 168  # fallback for unknown entity types
