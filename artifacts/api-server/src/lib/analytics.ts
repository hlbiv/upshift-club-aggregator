const STRIP_SUFFIXES = [
  "Football Club",
  "Soccer Club",
  "Soccer Academy",
  "Youth Soccer",
  "Youth Football",
  "Youth Academy",
  "Youth FC",
  "Youth SC",
  "Youth CF",
  "Junior",
  "Juniors",
  "Select",
  "Premier",
  "Elite",
  "Academy",
  "Athletic Club",
  "Athletics",
  "Athletic",
  "Futbol Club",
  "Futbol",
  "Sporting Club",
  "Sporting",
  "United",
  "Soccer",
  "Football",
  "Club",
  "FC",
  "SC",
  "CF",
  "FA",
];

const SUFFIX_PATTERN = STRIP_SUFFIXES
  .sort((a, b) => b.length - a.length)
  .map((s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
  .join("|");

const SUFFIX_RE = new RegExp(`\\b(${SUFFIX_PATTERN})\\b`, "gi");

export function normalizeClubName(name: string): string {
  return name
    .replace(SUFFIX_RE, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

export const PG_NORMALIZE_EXPR = `
lower(trim(regexp_replace(
  regexp_replace(
    regexp_replace(
      club_name_canonical,
      '\\m(Football Club|Soccer Club|Soccer Academy|Youth Soccer|Youth Football|Youth Academy|Youth FC|Youth SC|Youth CF|Juniors?|Select|Premier|Elite|Academy|Athletic Club|Athletics|Athletic|Futbol Club|Futbol|Sporting Club|Sporting|United|Soccer|Football|Club|FC|SC|CF|FA)\\M',
      '',
      'gi'
    ),
    '\\s+',' ','g'
  ),
  '^\\s+|\\s+$', '', 'g'
)))
`.trim();
