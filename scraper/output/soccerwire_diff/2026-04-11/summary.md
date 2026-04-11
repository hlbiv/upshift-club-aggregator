# SoccerWire Net-New Diff Report — 2026-04-11

Comparison of SoccerWire-sourced clubs against canonical clubs in `master.csv`.
Fuzzy-match threshold: DUPLICATE ≥ 88, NEAR_MATCH 75–87, NET_NEW < 75.

| State | SoccerWire Total | NET_NEW | NEAR_MATCH | DUPLICATE | NEAR_MATCH clubs (review) | DUPLICATE clubs (best canonical match) |
|-------|-----------------|---------|------------|-----------|--------------------------|----------------------------------------|
| HI | 6 | 6 | 0 | 0 | — | — |
| LA | 3 | 0 | 1 | 2 | Louisiana Fire SC ≈ 'Louisiana Elite' (83) | Baton Rouge SC → 'Baton Rouge' (100); Louisiana TDP Elite → 'Louisiana Elite' (88) |
| MA | 3 | 3 | 0 | 0 | — | — |
| MS | 3 | 1 | 2 | 0 | Mississippi Rush ≈ 'South Mississippi' (85); Mississippi Rush United ≈ 'South Mississippi' (85) | — |
| NE | 5 | 5 | 0 | 0 | — | — |
| RI | 1 | 1 | 0 | 0 | — | — |
| SC | 2 | 2 | 0 | 0 | — | — |
| WI | 5 | 5 | 0 | 0 | — | — |
| **TOTAL** | **28** | **23** | **3** | **2** | | |

## NET_NEW clubs by state

### HI

- **Hawaii Rush Big Island** (Hilo) — https://www.soccerwire.com/club/big-island-rush-sc/
- **Hawaii Rush** — https://www.soccerwire.com/club/hawaii-rush/
- **Hawaii Surf** — https://www.soccerwire.com/club/hawaii-surf/
- **Honolulu Bulls** — https://www.soccerwire.com/club/honolulu-bulls/
- **ALBION SC – Hawaii** — https://www.soccerwire.com/club/albion-sc-hawaii/
- **Kona Crush Academy** (Kailua-Kona) — https://www.soccerwire.com/club/kona-crush-academy/

### MA

- **FCUSA Massachusetts** (Plymouth) — https://www.soccerwire.com/club/fcusa-massachusetts/
- **GPS Massachusetts** (Waltham) — https://www.soccerwire.com/club/gps-massachusetts/
- **Boston Bolts** (Newton) — https://www.soccerwire.com/club/boston-bolts/

### MS

- **Tupelo FC** (Tupelo) — https://www.soccerwire.com/club/tupelo-fc/

### NE

- **Nebraska Select** (Kearney) — https://www.soccerwire.com/club/nebraska-select/
- **Omaha FC** — https://www.soccerwire.com/club/omaha-fc/
- **Sporting Nebraska** (Omaha) — https://www.soccerwire.com/club/sporting-omaha-fc/
- **Villarreal Nebraska Academy** (Lincoln) — https://www.soccerwire.com/club/villarreal-nebraska-academy/
- **Omaha United SC** (Omaha) — https://www.soccerwire.com/club/omaha-united-sc/

### RI

- **Rhode Island Surf** — https://www.soccerwire.com/club/rhode-island-surf/

### SC

- **South Carolina United FC** (Columbia) — https://www.soccerwire.com/club/south-carolina-united/
- **South Carolina Surf** (Charleston) — https://www.soccerwire.com/club/south-carolina-surf/

### WI

- **FC Wisconsin** (Germantown) — https://www.soccerwire.com/club/fc-wisconsin/
- **FC Wisconsin Eclipse** (Madison) — https://www.soccerwire.com/club/fc-wisconsin-eclipse/
- **Madison 56ers** (Madison) — https://www.soccerwire.com/club/madison-56ers/
- **Waukesha SC** (Waukesha) — https://www.soccerwire.com/club/waukesha-sc/
- **Rush Wisconsin** — https://www.soccerwire.com/club/rush-wisconsin/

## NEAR_MATCH clubs by state (needs human review)

### LA

- **Louisiana Fire SC** (Kenner) ≈ `Louisiana Elite` (score 83) — https://www.soccerwire.com/club/louisiana-fire/

### MS

- **Mississippi Rush** (Madison) ≈ `South Mississippi` (score 85) — https://www.soccerwire.com/club/mississippi-rush/
- **Mississippi Rush United** (Jackson) ≈ `South Mississippi` (score 85) — https://www.soccerwire.com/club/mississippi-rush-united/

## DUPLICATE clubs by state (already in master.csv)

### LA

- **Baton Rouge SC** → `Baton Rouge` (score 100) — https://www.soccerwire.com/club/baton-rouge-sc/
- **Louisiana TDP Elite** → `Louisiana Elite` (score 88) — https://www.soccerwire.com/club/louisiana-tdp-elite/

## Notes

- Generated: 2026-04-11
- Source: SoccerWire WP REST API + individual club pages
- Compared against: `output/master.csv` (canonical clubs, all sources)
- Classifications: NET_NEW=safe to append; NEAR_MATCH=needs human review; DUPLICATE=already covered
