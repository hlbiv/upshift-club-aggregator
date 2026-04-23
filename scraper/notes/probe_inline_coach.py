"""
One-off diagnostic script for Task #34.

Fetches a representative sample of D1 men's-soccer roster pages, runs the
current `extract_head_coach_from_html` against each, and classifies the
HTML for inline coach/staff markup the current extractor misses.

Output: prints a markdown-friendly report to stdout. Caller redirects to
scraper/notes/inline_coach_probe.md.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from extractors.ncaa_soccer_rosters import extract_head_coach_from_html  # noqa: E402

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

SAMPLE = [
    ("Portland", "https://portlandpilots.com/sports/mens-soccer/roster"),
    ("Stanford", "https://gostanford.com/sports/mens-soccer/roster"),
    ("Michigan", "https://mgoblue.com/sports/mens-soccer/roster"),
    ("Xavier", "https://goxavier.com/sports/mens-soccer/roster"),
    ("Seton Hall", "https://shupirates.com/sports/mens-soccer/roster"),
    ("Michigan State", "https://msuspartans.com/sports/mens-soccer/roster"),
    ("Cal", "https://calbears.com/sports/mens-soccer/roster"),
    ("Loyola Chicago", "https://loyolaramblers.com/sports/mens-soccer/roster"),
    ("Duke", "https://goduke.com/sports/mens-soccer/roster"),
    ("Virginia Tech", "https://hokiesports.com/sports/mens-soccer/roster"),
    ("UCLA", "https://uclabruins.com/sports/mens-soccer/roster"),
    ("Wake Forest", "https://godeacs.com/sports/mens-soccer/roster"),
    ("Gonzaga", "https://gozags.com/sports/mens-soccer/roster"),
    ("St. John's", "https://redstormsports.com/sports/mens-soccer/roster"),
    ("Purdue", "https://purduesports.com/sports/soccer/roster"),
    ("Washington", "https://gohuskies.com/sports/mens-soccer/roster"),
    ("Boston College", "https://bceagles.com/sports/mens-soccer/roster"),
    ("Georgetown", "https://guhoyas.com/sports/mens-soccer/roster"),
    ("LMU", "https://lmulions.com/sports/mens-soccer/roster"),
    ("Pepperdine", "https://pepperdinewaves.com/sports/mens-soccer/roster"),
    ("Saint Louis", "https://slubillikens.com/sports/mens-soccer/roster"),
    ("Syracuse", "https://cuse.com/sports/mens-soccer/roster"),
    ("SMU", "https://smumustangs.com/sports/mens-soccer/roster"),
    ("Santa Clara", "https://santaclarabroncos.com/sports/mens-soccer/roster"),
    ("Providence", "https://friars.com/sports/mens-soccer/roster"),
    ("DePaul", "https://depaulbluedemons.com/sports/mens-soccer/roster"),
    ("George Mason", "https://gomason.com/sports/mens-soccer/roster"),
    ("Ohio State", "https://ohiostatebuckeyes.com/sports/mens-soccer/roster"),
    ("Wake Forest dup", "https://godeacs.com/sports/mens-soccer/roster"),
    ("Gonzaga dup", "https://gozags.com/sports/mens-soccer/roster"),
]
# de-dup
seen = set()
SAMPLE = [(n, u) for n, u in SAMPLE if not (u in seen or seen.add(u))]

CMS_FINGERPRINTS = [
    ("sidearm", re.compile(r"sidearm", re.I)),
    ("nuxt", re.compile(r"__NUXT__|window\.__NUXT__|nuxt-data", re.I)),
    ("wmt", re.compile(r"wmtdigital|wp-content/themes/wmt", re.I)),
    ("nextjs", re.compile(r"__NEXT_DATA__", re.I)),
    ("presto", re.compile(r"prestosports|presto-sports", re.I)),
]

HEAD_COACH_TEXT_RE = re.compile(
    r"\bhead\s+(?:men'?s?|women'?s?)?\s*(?:soccer\s+)?coach\b", re.I
)


def classify_cms(html: str) -> str:
    snippet = html[:200_000]
    for name, regex in CMS_FINGERPRINTS:
        if regex.search(snippet):
            return name
    return "other"


def find_inline_coach_signals(html: str) -> dict:
    """Return diagnostic signals about inline coach markup on the page."""
    soup = BeautifulSoup(html, "html.parser")
    out = {
        "head_coach_text_count": 0,
        "head_coach_text_samples": [],
        "json_ld_person_with_head_coach": False,
        "json_ld_person_count": 0,
        "meta_head_coach": False,
        "staff_card_classes": [],
        "person_card_classes": [],
        "nuxt_payload_has_coach": False,
        "next_data_has_coach": False,
    }

    text = soup.get_text(" ", strip=True)
    matches = HEAD_COACH_TEXT_RE.findall(text)
    out["head_coach_text_count"] = len(matches)

    # Sample contexts around first 3 head-coach text hits
    for m in HEAD_COACH_TEXT_RE.finditer(text):
        if len(out["head_coach_text_samples"]) >= 3:
            break
        s = max(0, m.start() - 60)
        e = min(len(text), m.end() + 60)
        out["head_coach_text_samples"].append(text[s:e])

    # JSON-LD <script type="application/ld+json">
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            payload = json.loads(s.string or "")
        except Exception:
            continue

        def walk(obj):
            if isinstance(obj, dict):
                t = obj.get("@type")
                if t == "Person" or (isinstance(t, list) and "Person" in t):
                    out["json_ld_person_count"] += 1
                    title = (obj.get("jobTitle") or "").lower()
                    if "head coach" in title or "head soccer" in title:
                        out["json_ld_person_with_head_coach"] = True
                for v in obj.values():
                    walk(v)
            elif isinstance(obj, list):
                for v in obj:
                    walk(v)

        walk(payload)

    # <meta> tags
    for m in soup.find_all("meta"):
        v = " ".join(filter(None, [m.get("name", ""), m.get("property", ""), m.get("content", "")]))
        if "head coach" in v.lower():
            out["meta_head_coach"] = True
            break

    # Staff/person card class names with head-coach text inside
    sc_seen = Counter()
    pc_seen = Counter()
    for el in soup.find_all(class_=True):
        classes = el.get("class") or []
        cls_str = " ".join(classes).lower()
        if not cls_str:
            continue
        is_staff = any(t in cls_str for t in ["staff", "coach", "person", "card", "bio"])
        if not is_staff:
            continue
        # Inspect only small leaf-ish containers
        txt = el.get_text(" ", strip=True)
        if len(txt) > 800 or len(txt) < 5:
            continue
        if HEAD_COACH_TEXT_RE.search(txt):
            for c in classes:
                cl = c.lower()
                if any(t in cl for t in ["staff", "coach", "person", "bio"]):
                    sc_seen[cl] += 1
                if "card" in cl or "person" in cl:
                    pc_seen[cl] += 1

    out["staff_card_classes"] = sc_seen.most_common(8)
    out["person_card_classes"] = pc_seen.most_common(8)

    # __NUXT__ inline payload
    for s in soup.find_all("script"):
        body = s.string or ""
        if "__NUXT__" in body or "window.__NUXT__" in body:
            if "head coach" in body.lower() or "head_coach" in body.lower():
                out["nuxt_payload_has_coach"] = True
        if "__NEXT_DATA__" in body or s.get("id") == "__NEXT_DATA__":
            if "head coach" in body.lower() or "head_coach" in body.lower():
                out["next_data_has_coach"] = True

    return out


def main() -> int:
    sess = requests.Session()
    sess.headers.update({"User-Agent": UA, "Accept": "text/html,*/*"})

    rows = []
    for name, url in SAMPLE:
        try:
            r = sess.get(url, timeout=20, allow_redirects=True)
            html = r.text if r.status_code == 200 else ""
        except Exception as exc:
            html = ""
            print(f"# FETCH FAIL {name}: {exc}", file=sys.stderr)
        time.sleep(0.4)

        baseline = extract_head_coach_from_html(html) if html else None
        cms = classify_cms(html) if html else "fetch-fail"
        signals = find_inline_coach_signals(html) if html else {}

        rows.append({
            "name": name,
            "url": url,
            "status": r.status_code if html else "ERR",
            "bytes": len(html),
            "cms": cms,
            "current_extracts": bool(baseline),
            "current_name": (baseline or {}).get("name"),
            "signals": signals,
        })
        print(f"  ✓ {name:<22} cms={cms:<8} extract={'Y' if baseline else 'N'}  inline_text={signals.get('head_coach_text_count', 0)}", file=sys.stderr)

    # Print markdown report
    print("# Inline coach selector probe — diagnostic\n")
    print(f"Sample: {len(rows)} D1 men's soccer roster pages\n")

    cms_counts = Counter(r["cms"] for r in rows)
    print("## CMS distribution\n")
    for cms, n in cms_counts.most_common():
        print(f"- {cms}: {n}")
    print()

    extract_count = sum(1 for r in rows if r["current_extracts"])
    inline_text_count = sum(1 for r in rows if r["signals"].get("head_coach_text_count", 0) > 0)
    json_ld_count = sum(1 for r in rows if r["signals"].get("json_ld_person_with_head_coach"))
    nuxt_count = sum(1 for r in rows if r["signals"].get("nuxt_payload_has_coach"))
    next_count = sum(1 for r in rows if r["signals"].get("next_data_has_coach"))

    print("## Headline numbers\n")
    print(f"- Pages with current extractor hit: **{extract_count}/{len(rows)}**")
    print(f"- Pages with inline 'Head Coach' text anywhere: **{inline_text_count}/{len(rows)}**")
    print(f"- Pages with JSON-LD Person@jobTitle=Head Coach: **{json_ld_count}/{len(rows)}**")
    print(f"- Pages with __NUXT__ payload mentioning head coach: **{nuxt_count}/{len(rows)}**")
    print(f"- Pages with __NEXT_DATA__ mentioning head coach: **{next_count}/{len(rows)}**")
    print()

    print("## Per-page detail\n")
    for r in rows:
        s = r["signals"]
        print(f"### {r['name']} — `{r['url']}`")
        print(f"- status={r['status']}, bytes={r['bytes']}, cms=`{r['cms']}`")
        print(f"- current extractor: **{'HIT' if r['current_extracts'] else 'MISS'}**"
              f"{' → ' + r['current_name'] if r['current_name'] else ''}")
        if s:
            print(f"- inline 'Head Coach' text: {s['head_coach_text_count']}")
            if s["head_coach_text_samples"]:
                for sample in s["head_coach_text_samples"]:
                    print(f"  - sample: `{sample.strip()[:160]}`")
            print(f"- JSON-LD Person count={s['json_ld_person_count']}, with Head Coach={s['json_ld_person_with_head_coach']}")
            print(f"- meta head-coach: {s['meta_head_coach']}")
            print(f"- nuxt payload coach: {s['nuxt_payload_has_coach']}")
            print(f"- next data coach: {s['next_data_has_coach']}")
            if s["staff_card_classes"]:
                print(f"- staff card classes around head-coach text: {s['staff_card_classes'][:5]}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
