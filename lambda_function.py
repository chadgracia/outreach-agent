"""
outreach_draft_lambda.py  v3

Flow:
  1. Pick 5 random pending (respecting 7-day firm lockout)
  2. Research all 5 in parallel — fetch firm website via Brave-found URLs
  3. Bedrock evaluates each and assigns: firm_fit | owner_opportunistic | staff_fit | hold | skip
  4. Take all qualifying in priority order (firm_fit > staff_fit > owner_opportunistic),
     skipping any whose firm domain was already used by an earlier winner in this run
  5. Draft email matched to type (with verbatim trading URL enforcement)
  6. Send one digest per winner to Chad, update statuses in S3, loop until TARGET_WINNERS
     winners have been emailed or qualifying candidates run out

Email types:
  firm_fit / staff_fit  — pitch the institution, reference their mandate
  owner_opportunistic   — acknowledge firm isn't a fit, reach out to owner personally

Status values:
  pending | sent | hold | skip | responded | unsubscribed | in_pipeline

Env vars: BRAVE_API_KEY
"""

import csv
import io
import json
import logging
import os
import random
import re
import unicodedata
import urllib.parse
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

OUTREACH_BUCKET   = "gracia-outreach-queue"
OUTREACH_KEY      = "outreach_queue_clean.csv"
DEALS_BUCKET      = "fetched-leads"
DEALS_KEY         = "deals_data.json"
SES_SENDER        = "agent@agent.graciagroup.com"
CHAD_EMAIL        = "cgracia@rainmakersecurities.com"
BEDROCK_MODEL     = "us.anthropic.claude-sonnet-4-6"
MAIN_URL          = "https://trades.graciagroup.com/"
BRAVE_API_KEY     = os.environ.get("BRAVE_API_KEY", "")
FIRM_LOCKOUT_DAYS = 7
BATCH_SIZE        = 5
TARGET_WINNERS    = 5
MAX_BATCHES       = 15

# Priority order for winner selection
TYPE_PRIORITY = {"firm_fit": 0, "staff_fit": 1, "owner_opportunistic": 2}


# ── S3 ────────────────────────────────────────────────────────────────────────

def load_csv(bucket, key):
    s3     = boto3.client("s3")
    obj    = s3.get_object(Bucket=bucket, Key=key)
    raw    = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(raw))
    return list(reader), reader.fieldnames


def save_csv(bucket, key, rows, fieldnames):
    buf    = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    boto3.client("s3").put_object(
        Bucket=bucket, Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )


# Stage IDs that count as "active" for outreach-context purposes:
# Inquiry, Firm, Matched, Confirm, Hold.
ACTIVE_DEAL_STAGE_IDS = {2109142, 111800, 2381534, 2388323, 2094373}


def _coerce_stage_id(deal):
    """Pull a stage id off a deal record regardless of how it's nested.
    Returns int or None."""
    candidates = [
        deal.get("stage_id"),
        deal.get("stageId"),
        deal.get("stage"),
        (deal.get("stage") or {}).get("id") if isinstance(deal.get("stage"), dict) else None,
        (deal.get("pipeline_stage") or {}).get("id") if isinstance(deal.get("pipeline_stage"), dict) else None,
    ]
    for v in candidates:
        if v is None:
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    return None


def _coerce_str(value):
    """Pull a string out of a possibly-nested {name: ...} structure."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for k in ("name", "title", "label", "value"):
            v = value.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""


def load_active_deals_summary():
    """Load deals_data.json from S3, filter to ACTIVE_DEAL_STAGE_IDS, and
    produce a compact list of "Company | Industry | Buy/Sell" strings.
    Returns [] if the file is missing or malformed."""
    try:
        s3   = boto3.client("s3")
        obj  = s3.get_object(Bucket=DEALS_BUCKET, Key=DEALS_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8", errors="ignore"))
    except Exception as e:
        logger.warning(
            f"Could not load deals from s3://{DEALS_BUCKET}/{DEALS_KEY}: {e}; "
            f"continuing without active_deals_summary"
        )
        return []

    if isinstance(data, list):
        deals = data
    elif isinstance(data, dict):
        deals = data.get("deals") or data.get("data") or data.get("results") or []
    else:
        deals = []

    summary = []
    for d in deals:
        if not isinstance(d, dict):
            continue
        stage_id = _coerce_stage_id(d)
        if stage_id not in ACTIVE_DEAL_STAGE_IDS:
            continue

        company = (
            _coerce_str(d.get("company_name"))
            or _coerce_str(d.get("company"))
            or _coerce_str(d.get("issuer"))
        )
        industry = (
            _coerce_str(d.get("industry"))
            or _coerce_str(d.get("company_industry"))
            or _coerce_str(d.get("sector"))
            or _coerce_str((d.get("company") or {}).get("industry") if isinstance(d.get("company"), dict) else None)
        )
        deal_type_raw = (
            _coerce_str(d.get("deal_type"))
            or _coerce_str(d.get("type"))
            or _coerce_str(d.get("side"))
            or _coerce_str(d.get("direction"))
        ).lower()
        if deal_type_raw in {"buy", "buyer", "buy-side", "buyside", "bid"}:
            deal_type = "Buy"
        elif deal_type_raw in {"sell", "seller", "sell-side", "sellside", "ask", "offer"}:
            deal_type = "Sell"
        else:
            deal_type = ""

        if not (company and industry and deal_type):
            continue
        summary.append(f"{company} | {industry} | {deal_type}")

    # Dedupe while preserving order.
    seen = set()
    deduped = []
    for line in summary:
        if line not in seen:
            seen.add(line)
            deduped.append(line)

    logger.info(f"Active deals summary: {len(deduped)} entries (from {len(deals)} total)")
    return deduped


# ── Web helpers ───────────────────────────────────────────────────────────────

NAV_TERMS = {
    "about", "team", "portfolio", "contact", "menu", "home",
    "services", "resources", "careers", "blog", "news",
    "login", "log in", "sign in", "sign up", "subscribe",
    "investments", "holdings", "companies", "people",
    "search", "press", "media", "events",
}


def fetch_page(url, max_chars=3000):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            raw  = resp.read().decode("utf-8", errors="ignore")
        # Drop <script> and <style> blocks (and their contents) before
        # stripping HTML tags, so we don't leak JS or JSON-LD into research.
        cleaned = re.sub(r"<script\b[^>]*>.*?</script>", " ", raw,
                         flags=re.DOTALL | re.IGNORECASE)
        cleaned = re.sub(r"<style\b[^>]*>.*?</style>", " ", cleaned,
                         flags=re.DOTALL | re.IGNORECASE)
        # Insert newlines at block boundaries so we can filter nav noise per line.
        cleaned = re.sub(
            r"</?(p|div|li|ul|ol|nav|header|footer|section|article|h[1-6]|tr|td|br)\b[^>]*>",
            "\n", cleaned, flags=re.IGNORECASE,
        )
        text = re.sub(r"<[^>]+>", " ", cleaned)
        kept = []
        for line in text.split("\n"):
            line = re.sub(r"[ \t]+", " ", line).strip()
            if not line:
                continue
            words = line.split()
            if len(words) < 6:
                low = line.lower().rstrip(".,;:!?")
                if low in NAV_TERMS:
                    continue
                # All-capitalized short lines (e.g. "About Us Team Portfolio") look like nav.
                if all(w[:1].isupper() for w in words if w):
                    continue
            kept.append(line)
        text = " ".join(kept)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:max_chars]
    except Exception as e:
        logger.info(f"fetch_page failed {url}: {e}")
        return ""


def brave_search(query, count=5):
    url = (
        "https://api.search.brave.com/res/v1/web/search"
        f"?q={urllib.parse.quote(query)}&count={count}"
    )
    req = urllib.request.Request(url, headers={
        "Accept":               "application/json",
        "X-Subscription-Token": BRAVE_API_KEY,
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        results = []
        for item in (data.get("web", {}).get("results") or [])[:count]:
            results.append({
                "title":    item.get("title", ""),
                "snippet":  item.get("description", ""),
                "url":      item.get("url", ""),
                "page_age": item.get("page_age", ""),  # ISO 8601 if present
                "age":      item.get("age", ""),       # human-readable e.g. "3 days ago"
            })
        logger.info(f"Brave '{query[:60]}': {len(results)} results")
        return results
    except Exception as e:
        logger.warning(f"Brave failed: {e}")
        return []


PORTFOLIO_KEYWORDS = ["portfolio", "investments", "companies", "holdings"]
USEFUL_KEYWORDS    = PORTFOLIO_KEYWORDS + [
    "about", "invest", "focus", "strategy", "approach", "team", "what-we-do"
]


def best_url_from_results(domain, results, keywords=None):
    """Pick the most informative URL Brave found for this domain."""
    keywords = keywords if keywords is not None else USEFUL_KEYWORDS
    # Prefer pages from their own domain with useful keywords
    for kw in keywords:
        for r in results:
            u = r.get("url", "").lower()
            if domain in u and kw in u:
                return r["url"]
    # Fall back to any result from their domain
    for r in results:
        if domain in r.get("url", "").lower():
            return r["url"]
    return None


# Words/tokens that look like firm names but are almost always navigation / generic.
_FIRM_NAME_BLACKLIST = {
    "about", "about us", "home", "contact", "contact us", "team", "our team",
    "menu", "log in", "sign in", "sign up", "privacy policy", "terms of service",
    "family office", "private equity", "wealth management", "asset management",
    "investment management", "the team", "our firm",
}

# Strip trailing legal entity suffixes when they appear at the end of an extracted
# phrase (so e.g. "Hall Capital LLC" becomes "Hall Capital"). Note: "Group" and
# "Holdings" are intentionally NOT included — they're more often part of the
# canonical firm name (e.g. Casimir Holdings, Carlyle Group).
_LEGAL_SUFFIX_RE = re.compile(
    r"\s+(LLC|L\.L\.C\.?|Inc\.?|Incorporated|Ltd\.?|Limited|LP|L\.P\.?|"
    r"LLP|Co\.?|Corp\.?|Corporation)$",
    re.IGNORECASE,
)

# Two-to-five capitalized tokens in a row, allowing acronyms and ampersands.
_FIRM_NAME_RE = re.compile(r"\b([A-Z][A-Za-z&]*(?:\s+[A-Z][A-Za-z&]*){1,4})\b")


def _extract_firm_name(results, domain):
    """Pick the most-common capitalized multi-word phrase appearing in titles
    and snippets that overlaps with the domain stem. Returns "" if nothing
    plausible is found."""
    if not results or not domain:
        return ""
    domain_stem = re.sub(r"[^a-z]", "", domain.split(".")[0].lower())
    if not domain_stem:
        return ""

    scores = {}
    for r in results:
        text = f"{r.get('title','')} . {r.get('snippet','')}"
        for phrase in _FIRM_NAME_RE.findall(text):
            phrase = phrase.strip()
            low    = phrase.lower()
            if low in _FIRM_NAME_BLACKLIST:
                continue
            words = phrase.split()
            if len(words) < 2 or len(words) > 5:
                continue
            phrase_letters = re.sub(r"[^a-z]", "", low)
            if not phrase_letters:
                continue
            # Strong signal: domain stem is a substring of the phrase letters.
            if domain_stem in phrase_letters or phrase_letters in domain_stem:
                weight = 5
            elif domain_stem[:4] and domain_stem[:4] in phrase_letters:
                weight = 2
            else:
                continue  # phrase doesn't look related to this domain
            scores[phrase] = scores.get(phrase, 0) + weight

    if not scores:
        return ""

    # Highest weighted-frequency wins; ties broken by shorter phrase
    # (longer phrases tend to include legal suffixes / extra noise).
    best = max(scores.items(), key=lambda kv: (kv[1], -len(kv[0])))[0]
    cleaned = _LEGAL_SUFFIX_RE.sub("", best).strip()
    return cleaned or best


def research_firm(row):
    email  = row.get("Email", "").strip()
    domain = email.split("@")[-1].lower() if "@" in email else ""
    first  = row.get("First Name", "").strip()
    last   = row.get("Last Name", "").strip()
    firm_raw = row.get("Firm", "").strip()

    # Initial firm name: use the CSV value if any, else a title-cased domain stem.
    if firm_raw:
        initial_firm_query = firm_raw
    else:
        stem = domain.split(".")[0] if domain else ""
        initial_firm_query = " ".join(p.capitalize() for p in re.split(r"[-_]", stem)) if stem else ""
        if initial_firm_query:
            logger.warning(
                f"Firm column empty for {email!r}; bootstrapping with domain-derived "
                f"firm_query={initial_firm_query!r}"
            )

    # Initial primary search — also serves as snippet source for firm-name extraction.
    results = brave_search(
        f'"{initial_firm_query}" "{domain}" family office investments', count=6
    ) if (initial_firm_query or domain) else []

    # Extract a cleaner firm name from the snippets we just got.
    extracted = _extract_firm_name(results, domain)
    firm_query = extracted or initial_firm_query

    if extracted and extracted.lower() != initial_firm_query.lower():
        logger.info(
            f"Firm name resolved from snippets: csv={firm_raw!r} "
            f"extracted={extracted!r} (was using {initial_firm_query!r}) for {domain}"
        )
        # Re-run primary search with the cleaned name so downstream content
        # is anchored on the canonical firm name, not the malformed CSV value.
        fresh = brave_search(
            f'"{firm_query}" "{domain}" family office investments', count=6
        )
        if fresh:
            results = fresh

    # Portfolio search uses the cleaned firm name.
    portfolio_results = brave_search(
        f'"{firm_query}" portfolio OR investments OR companies', count=6
    ) if firm_query else []

    # Use the cleaned firm name everywhere downstream (display, Bedrock prompts).
    firm = firm_query or firm_raw

    # Fetch homepage
    home_url = f"https://{domain}" if domain else ""
    homepage = fetch_page(home_url) if domain else ""

    # Fetch best secondary page if different from homepage
    best_url   = best_url_from_results(domain, results)
    secondary  = fetch_page(best_url) if best_url and best_url != home_url else ""

    # Fetch a dedicated portfolio/investments/companies/holdings page if we can find one
    # that is distinct from homepage and secondary.
    portfolio_url = best_url_from_results(
        domain, results + portfolio_results, keywords=PORTFOLIO_KEYWORDS
    )
    if portfolio_url and portfolio_url not in {home_url, best_url}:
        portfolio_content = fetch_page(portfolio_url)
    else:
        portfolio_content = ""

    snippets = "\n".join(f"- {r['title']}: {r['snippet']}" for r in (results + portfolio_results))

    # Combine: prefer secondary page, fall back to homepage, fall back to snippets
    web_content = secondary or homepage or snippets

    logger.info(
        f"Research {first} {last} @ {domain}: "
        f"homepage={len(homepage)}c, secondary={len(secondary)}c, "
        f"portfolio={len(portfolio_content)}c, snippets={len(snippets)}c"
    )
    return {
        "web_content":       web_content,
        "portfolio_content": portfolio_content,
        "snippets":          snippets,
        "domain":            domain,
        "firm":              firm,
    }


# ── Bedrock ───────────────────────────────────────────────────────────────────

def bedrock_call(prompt, max_tokens=800):
    bedrock  = boto3.client("bedrock-runtime", region_name="us-east-1")
    response = bedrock.invoke_model(
        modelId     = BEDROCK_MODEL,
        body        = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens":        max_tokens,
            "messages":          [{"role": "user", "content": prompt}],
        }),
        contentType = "application/json",
        accept      = "application/json",
    )
    raw = json.loads(response["body"].read())
    return raw["content"][0]["text"].strip()


def bedrock_json(prompt, max_tokens=800):
    text = bedrock_call(prompt, max_tokens)
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    try:
        return json.loads(text.strip())
    except Exception as e:
        logger.warning(f"JSON parse error: {e} | raw: {text[:300]}")
        return {}


# ── Evaluation ────────────────────────────────────────────────────────────────

def evaluate_candidates(candidates, research_list):
    """
    Bedrock evaluates each candidate and assigns a type + angle.
    Returns list of {index, type, angle} sorted by priority.
    """
    profiles = ""
    for i, (row, res) in enumerate(zip(candidates, research_list)):
        about_text     = (res.get("web_content") or res.get("snippets") or "No content found.")[:600]
        portfolio_text = (res.get("portfolio_content") or "")[:600] or "(no portfolio page found)"
        profiles += f"""
--- Candidate {i} ---
Name:      {row.get('First Name','')} {row.get('Last Name','')}
Title:     {row.get('Job Title','')}
Firm:      {res['firm']} ({res['domain']})
About/web: {about_text}
Portfolio: {portfolio_text}
"""

    prompt = f"""You are evaluating prospects for a broker who sells pre-IPO secondary shares
in private tech companies (Anthropic, SpaceX, Anduril, xAI, etc.) — direct investments only, no SPVs or funds.

Evaluate each candidate and assign ONE of these types:

  firm_fit
    The firm itself is a family office that invests passively in private tech/growth companies.
    They don't need board seats or control. Signals: growth equity, tech, innovation, alternatives,
    private markets, venture exposure mentioned. Staff at these firms (VP, Associate, analyst) also
    get firm_fit — they are legitimate contacts for deal flow even if not the decision maker.

  owner_opportunistic
    The firm is a family office but the mandate doesn't match (e.g. control PE, real estate,
    mid-market buyouts, or unclear). However, this specific person is a FOUNDER, OWNER, CEO,
    or PRINCIPAL whose own money is being managed. Even if the firm doesn't buy minority tech
    stakes, they personally might invest $100K-$1M in Anthropic or SpaceX opportunistically.
    Use this type for senior principals at ANY family office regardless of mandate.

  hold
    The entity is a VC fund, wealth advisor managing third-party capital, bank, pension fund,
    endowment, or large institution. Not relevant now but not worth burning.

  skip
    Definitively not relevant: real estate only, infrastructure debt, non-investor (law firm,
    accountant, service provider), or no signals of investment activity at all.

EVIDENCE RULES — read carefully:
- Look at ACTUAL investment examples on the Portfolio section, not just mandate language
  from the about page. "Private equity" / "growth equity" wording on its own is NOT enough
  to justify firm_fit if the visible portfolio doesn't back it up.
- If portfolio companies are visible and are clearly NON-tech (local businesses,
  middle-market industrial, real estate, consumer services, regional manufacturing,
  laundromats, restaurants, dealerships, etc.), DOWNGRADE accordingly:
    * The PERSON is a founder/owner/CEO/principal at the firm → owner_opportunistic
    * The PERSON is just an employee/staff → hold
- A real-estate-only firm owner = skip (real estate is the business, not a family office wrapper).
- A control-PE firm owner = owner_opportunistic (not skip).
- Junior staff (Analyst, Associate) at a firm whose ACTUAL portfolio is tech/growth = firm_fit.
- Junior staff at a non-qualifying firm = hold (or skip if the firm is clearly out of scope).
- When in doubt between hold and skip, choose hold.

The "angle" field MUST contain three things, separated by " | ":
  (a) What the firm actually invests in based on portfolio evidence (one phrase, concrete).
  (b) The specific angle most likely to get a response from THIS person.
  (c) One non-obvious biographical or firm detail worth Chad knowing — unusual background,
      career path, firm origin story, surprising portfolio company, etc. If nothing
      non-obvious is visible in the research, write "no notable detail in research".

Candidates:
{profiles}

Respond ONLY with valid JSON:
{{
  "evaluations": [
    {{
      "index": 0,
      "type": "firm_fit|owner_opportunistic|hold|skip",
      "angle": "<(a) actual investments | (b) angle for this person | (c) non-obvious detail>"
    }},
    ...
  ]
}}

UNIQUENESS NOTE: when later drafting outreach emails for the qualifying candidates,
no two emails in this run may share the same opening phrase, sentence structure, or
framing. Vary the angle materially across candidates."""

    result = bedrock_json(prompt, max_tokens=700)
    evaluations = result.get("evaluations", [])

    # Sort qualifying candidates by priority
    qualifying = [
        e for e in evaluations
        if e.get("type") in TYPE_PRIORITY
    ]
    qualifying.sort(key=lambda e: TYPE_PRIORITY.get(e.get("type"), 99))
    return evaluations, qualifying


# ── Strategic research summary ────────────────────────────────────────────────

def summarize_research(row, research, angle, market_context_text=""):
    """Bedrock-generated 3-4 sentence strategic summary used in the digest header."""
    first = row.get("First Name", "").strip()
    last  = row.get("Last Name", "").strip()
    title = row.get("Job Title", "").strip()
    firm  = research.get("firm", "")

    about_text     = (research.get("web_content") or research.get("snippets") or "")[:1200]
    portfolio_text = (research.get("portfolio_content") or "")[:1200]

    prompt = f"""Summarize the firm and the prospect for Chad's awareness in 3-4 sentences max.
Do NOT write an email. Do NOT pitch. Strategic notes only.

Cover:
  1. What the firm ACTUALLY does, based on evidence (especially portfolio companies),
     not their self-description.
  2. The specific angle most likely to get a response from this person.
  3. One non-obvious detail worth knowing — biographical, portfolio, origin story.
     If nothing non-obvious is visible, say so plainly; do not invent.

If market_context is provided and any item in it is directly relevant to this firm,
you may reference it in one phrase. Otherwise ignore it.

PROSPECT
  Name:  {first} {last}
  Title: {title}
  Firm:  {firm}

PRIOR ANGLE NOTES (may be incomplete):
{angle}

ABOUT / WEB:
{about_text or '(none)'}

PORTFOLIO:
{portfolio_text or '(none)'}

MARKET CONTEXT (optional):
{(market_context_text or '')[:600] or '(none)'}

Respond with plain text. No JSON. No headers. No bullet points. 3-4 sentences."""
    try:
        return bedrock_call(prompt, max_tokens=350).strip()
    except Exception as e:
        logger.warning(f"summarize_research failed for {first} {last}: {e}")
        return ""


# ── Market context (once per invocation) ──────────────────────────────────────

REPUTABLE_OUTLETS = (
    "wsj.com", "bloomberg.com", "reuters.com", "ft.com",
    "techcrunch.com", "fortune.com", "forbes.com", "axios.com",
    "cnbc.com", "businessinsider.com", "pitchbook.com",
)

MARKET_CONTEXT_MAX_AGE_DAYS = 14


def _parse_brave_page_age(page_age):
    """Parse Brave's page_age field (ISO 8601 string) to a UTC datetime.
    Returns None if unparseable or empty."""
    if not page_age:
        return None
    try:
        # Brave returns e.g. "2026-04-28T13:45:00" or with trailing "Z" / offset.
        s = page_age.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def fetch_market_context():
    """Run a few Brave queries on private/secondary market news; HARD-FILTER
    by publication date (must be within MARKET_CONTEXT_MAX_AGE_DAYS) before
    anything is passed to Bedrock. Fetch the top page from a reputable outlet.
    Returns a dict with snippets + page content; both empty if no fresh
    reputable result is found."""
    queries = [
        "private secondary market 2026",
        "pre-IPO transactions news 2026",
        "private markets outlook 2026",
    ]
    cutoff       = datetime.now(timezone.utc) - timedelta(days=MARKET_CONTEXT_MAX_AGE_DAYS)
    snippets     = []
    page_content = ""
    page_source  = ""
    dropped_stale     = 0
    dropped_undated   = 0
    dropped_low_trust = 0

    for q in queries:
        results = brave_search(q, count=5)
        for r in results:
            url = (r.get("url") or "").lower()
            if not any(o in url for o in REPUTABLE_OUTLETS):
                dropped_low_trust += 1
                continue
            published = _parse_brave_page_age(r.get("page_age", ""))
            if published is None:
                # No date → treat as stale; we cannot prove freshness.
                dropped_undated += 1
                continue
            if published < cutoff:
                dropped_stale += 1
                continue
            # Passed filters: include this result.
            snippets.append(
                f"- ({q}) [{published.strftime('%Y-%m-%d')}] "
                f"{r.get('title','')}: {r.get('snippet','')}"
            )
            if not page_content:
                page_content = fetch_page(r["url"])
                page_source  = r["url"]
            break

    logger.info(
        f"Market context: kept_snippets={len(snippets)} "
        f"page_content={len(page_content)}c source={page_source or '(none)'} "
        f"dropped_stale={dropped_stale} dropped_undated={dropped_undated} "
        f"dropped_low_trust={dropped_low_trust}"
    )

    # If nothing fresh-and-reputable made it through, return empty so the
    # middle paragraph is omitted entirely rather than citing stale data.
    if not snippets and not page_content:
        return {"snippets": "", "page_content": "", "source": ""}

    return {
        "snippets":     "\n".join(snippets),
        "page_content": page_content,
        "source":       page_source,
    }


def market_context_text(market_context):
    if not market_context:
        return ""
    return (market_context.get("page_content") or market_context.get("snippets") or "").strip()


# ── Email drafting ────────────────────────────────────────────────────────────

def draft_email(row, email_type, angle, research, market_context=None,
                prior_first_sentences=None, active_deals_summary=None):
    first = row.get("First Name", "").strip()
    last  = row.get("Last Name", "").strip()
    title = row.get("Job Title", "").strip()
    firm  = row.get("Firm", "").strip()

    if email_type == "owner_opportunistic":
        type_instruction = f"""This person runs a family office whose mandate does not directly
match what Chad offers. The email should:
- Explicitly acknowledge that the firm's focus isn't aligned with what Chad does
- Pivot to a personal, opportunistic angle — they likely have personal capital outside the firm mandate
- Be brief and direct — no need to over-explain
- Example tone: "I know {firm} is focused on [their thing], so this probably isn't relevant
  institutionally, but thought you might find it interesting on a personal basis."
"""
    else:
        type_instruction = f"""This is a family office contact. The email should:
- Reference their investment mandate naturally and briefly (not quoting their website)
- Position Chad's offering as relevant to their existing approach
- Example tone: "Given your focus on growth equity, wanted to put myself on your radar."
"""

    market_text = market_context_text(market_context)
    if market_text:
        market_block = (
            "MARKET CONTEXT (raw research; ground the middle paragraph in this — "
            "no invented facts, specific dollar figures and company names from "
            "respected sources are fine):\n"
            f"{market_text[:1200]}"
        )
        market_paragraph_instruction = (
            "Middle paragraph: 2-3 sentences grounded in the MARKET CONTEXT above, "
            "connecting current pre-IPO/secondary market conditions to why Chad is "
            "reaching out NOW. This paragraph MUST vary in angle and framing across "
            "the 5 drafts in this run — same source material, different emphasis "
            "each time. No invented facts. If a figure or company name from the "
            "context is relevant, use it; do NOT make one up."
        )
    else:
        market_block = "MARKET CONTEXT: (none — omit the middle paragraph entirely)"
        market_paragraph_instruction = (
            "Middle paragraph: OMIT entirely. No market paragraph this run. "
            "Go straight from sentence 2 to sentence 3."
        )

    portfolio_text = (research.get("portfolio_content") or "")[:400]
    research_text  = (research.get("web_content") or research.get("snippets") or "")[:500]

    prior_block = ""
    if prior_first_sentences:
        joined = "\n".join(f"- {s}" for s in prior_first_sentences if s)
        if joined:
            prior_block = (
                "ALREADY-USED OPENING SENTENCES IN THIS RUN (your sentence 1 must "
                f"be materially different from all of these):\n{joined}\n"
            )

    deals_block = ""
    if active_deals_summary:
        deals_lines = "\n".join(f"- {line}" for line in active_deals_summary)
        deals_block = f"""ACTIVE DEALS — what Chad is currently working on (Company | Industry | Buy/Sell):
{deals_lines}

Given this list and what you know about this prospect's firm, sector, geography,
and portfolio: identify whether there is a GENUINE specific connection between
what Chad is offering and what this person does or cares about. If yes, use that
connection naturally to inform sentence 1 (the specific opener) and the overall
tone. If no real connection exists, use a different specific non-obvious opener
instead — DO NOT manufacture a connection that isn't there. NEVER mention any
specific deal or company from the ACTIVE DEALS list by name in the email body;
the list is for your reasoning only.
"""

    prompt = f"""Write a cold outreach email from Chad Gracia to {first} {last}, {title} at {firm}.

WHAT CHAD DOES: He specializes in secondary transactions in pre-IPO private tech companies like
Anthropic, SpaceX, Anduril and xAI. He also sometimes has early access to seed rounds before
they become well known. Trading page: {MAIN_URL}

WHY THIS PERSON (use to inform tone only, do NOT quote back):
{angle}

FIRM RESEARCH (use to inform tone only, do NOT quote back):
{research_text}

PORTFOLIO EVIDENCE (specific holdings; one detail here is fair game for sentence 1):
{portfolio_text or '(none)'}

{market_block}

{deals_block}{prior_block}EMAIL TYPE INSTRUCTIONS:
{type_instruction}

EMAIL STRUCTURE — produce exactly this, in order:
  Dear {first},
  <blank line>
  <Sentence 1: specific opener>
  <Sentence 2: what Chad does, locked structure>
  <Market context paragraph: 2-3 sentences, varied — OR omit if no context>
  <Sentence 3: soft close with link>
  Sincerely,

Sentence 1: Open with something specific and non-obvious about THIS person or
            firm — drawn from their background, firm origin story, a specific
            portfolio company, an unusual credential, OR (if a genuine
            connection exists per the ACTIVE DEALS reasoning above) the
            sector / company-type overlap with what Chad is currently
            working on. It must NOT be a generic "given your focus on X"
            opener. It must NOT be flattering or fawning. NEVER name a
            specific deal from the ACTIVE DEALS list. Sound like Chad
            noticed something real. No two emails in this run may open the
            same way (see ALREADY-USED list above if present).

Sentence 2: Rewritten fresh each time, but the structure is locked. State that Chad
            specializes in secondary transactions in private companies and name at
            least these six companies: Anthropic, SpaceX, Anduril, AMI Labs,
            Project Prometheus, and Hark Labs. Also weave in that he sometimes has
            access to primary rounds and data rooms. Sound like a confident human
            wrote it quickly, not like a pitch deck. Vary the order and phrasing
            of the company names across emails so they don't all sound identical.
            Hard rules for THIS sentence: no "before it gets noisy", no "before
            it gets crowded", no "alpha", no "deal flow", no "proprietary", no
            "curated", no "exclusive access", no "unique opportunity", no
            "space" used as a noun (e.g. "the private markets space").

Market paragraph: {market_paragraph_instruction}

Sentence 3: Soft close that MUST contain the full URL {MAIN_URL} verbatim. Do not paraphrase it,
            do not shorten it, do not drop the trailing slash, do not wrap it in markdown.
            The literal string {MAIN_URL} must appear in this sentence as written.

Sign-off:   The literal text "Sincerely," on its own line. Do NOT write any
            name after it. Do NOT write "Chad", "Chad Gracia", or any other
            name anywhere in the body. The recipient pastes their own
            signature manually.

HARD RULES — violation means rejected:
- NO dashes of any kind used as punctuation (no em-dash, no en-dash, no " - " between clauses)
- NO phone call offers, NO "schedule a call", NO "hop on a call", NO "would love to connect"
- NO "excited", "thrilled", "leverage", "synergy", "reach out", "touch base", "circle back"
- NO parroting their website language back at them
- NO deal URLs or specific deal mentions
- Do NOT mention Rainmaker Securities in the body
- Do NOT say "FINRA broker"
- Sound like a smart human wrote it quickly, not a marketing department
- Vary vocabulary, sentence length, and angle vs. any prior emails in this run

Respond ONLY with valid JSON:
{{
  "subject": "<short subject, not salesy>",
  "body": "<salutation, blank line, sentence 1, sentence 2, optional market paragraph, sentence 3, sign-off; plain text; newlines as \\n>"
}}"""

    draft = bedrock_json(prompt, max_tokens=600)
    body  = draft.get("body", "") or ""

    if MAIN_URL not in body:
        logger.warning(
            f"Draft for {first} {last} missing URL {MAIN_URL!r} on first try; redrafting"
        )
        retry_prompt = (
            prompt
            + f"\n\nIMPORTANT: Your previous attempt did not include the literal string "
            f"{MAIN_URL} in the body. Rewrite the email and include {MAIN_URL} verbatim in "
            f"sentence 3."
        )
        draft = bedrock_json(retry_prompt, max_tokens=600)
        body  = draft.get("body", "") or ""

        if MAIN_URL not in body:
            logger.warning(
                f"Draft for {first} {last} still missing URL {MAIN_URL!r} after retry; "
                f"appending manually before sign-off"
            )
            body = _append_url_before_signoff(body, MAIN_URL)

    # First-sentence dedup: if the opener matches a prior winner's opener, regenerate once.
    fs = _first_sentence(body)
    if fs and prior_first_sentences and any(
        _normalize_sentence(fs) == _normalize_sentence(p) for p in prior_first_sentences if p
    ):
        logger.warning(
            f"Draft for {first} {last} duplicates a prior opening sentence; regenerating"
        )
        dedup_prompt = (
            prompt
            + "\n\nIMPORTANT: Your previous sentence 1 was nearly identical to one "
            "already used in this run. Rewrite the whole email with a materially "
            "different sentence 1 — different angle, different opening words, "
            "different structure."
        )
        draft = bedrock_json(dedup_prompt, max_tokens=600)
        body  = draft.get("body", "") or ""
        if MAIN_URL not in body:
            body = _append_url_before_signoff(body, MAIN_URL)

    draft["body"] = _strip_signoff_name(body)
    return draft


def _first_sentence(body):
    """Extract the first sentence after the salutation."""
    if not body:
        return ""
    for line in body.split("\n"):
        s = line.strip()
        if not s or s.lower().startswith("dear"):
            continue
        m = re.match(r"(.+?[.!?])(\s|$)", s)
        return (m.group(1) if m else s).strip()
    return ""


def _normalize_sentence(s):
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", s.lower())).strip()


def _append_url_before_signoff(body, url):
    """Insert the URL on its own line before the sign-off line."""
    lines = body.split("\n")
    signoff_idx = None
    for i in range(len(lines) - 1, -1, -1):
        s = lines[i].strip().lower()
        if s.startswith("sincerely") or s in {"chad", "chad,", "chad gracia"}:
            signoff_idx = i
            break
    if signoff_idx is None:
        return body.rstrip() + "\n\n" + url
    return "\n".join(lines[:signoff_idx] + [url, ""] + lines[signoff_idx:])


def _strip_signoff_name(body):
    """Remove any 'Chad Gracia' (or bare 'Chad') line that follows 'Sincerely,'.
    Defensive cleanup in case the model appends a name despite instructions."""
    if not body:
        return body
    cleaned = re.sub(r"\bChad\s+Gracia\b", "", body)
    lines = cleaned.split("\n")
    out = []
    prev_was_signoff = False
    for line in lines:
        stripped = line.strip()
        if prev_was_signoff and stripped.lower() in {"chad", "chad,", "chad gracia"}:
            prev_was_signoff = False
            continue
        out.append(line)
        prev_was_signoff = stripped.lower().startswith("sincerely")
    return "\n".join(out).rstrip() + "\n"


# ── SES ───────────────────────────────────────────────────────────────────────

def _ascii_fold(s):
    """NFKD-normalize then strip non-ASCII so e.g. 'joão' → 'joao', 'é' → 'e'.
    Used only for safe display in the digest body; original strings are not
    modified."""
    if not s:
        return s
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")


def send_digest(row, email_type, angle, research, draft, research_summary=""):
    ses   = boto3.client("ses", region_name="us-east-1")
    first = row.get("First Name", "")
    last  = row.get("Last Name", "")
    firm  = row.get("Firm", "")
    email = row.get("Email", "")

    # Display-only fold so non-ASCII characters in the prospect email (e.g.
    # joão@firm.com) don't break SES when interpolated into the digest body.
    # The actual SES Destination is CHAD_EMAIL, never the prospect's address.
    email_display = _ascii_fold(email)

    summary_text = (research_summary or "").strip() or "(no summary generated)"

    body = f"""OUTREACH DRAFT — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}

PROSPECT
  Name:       {first} {last}
  Title:      {row.get('Job Title', '')}
  Firm:       {firm}
  Email:      {email_display}
  Type:       {email_type}
  Why:        {angle}

RESEARCH SUMMARY
{summary_text}

{'=' * 50}
DRAFT EMAIL

{email_display}
{draft.get('subject', '(none)')}

{draft.get('body', '(no body)')}
{'=' * 50}

To update: email agent@ with "mark {email_display} as responded" or "mark {email_display} as skip"
"""

    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [CHAD_EMAIL]},
        Message={
            "Subject": {"Data": f"Outreach draft: {first} {last} ({email_type}) — {firm}"},
            "Body":    {"Text": {"Data": body}},
        },
    )
    logger.info(f"Digest sent for {first} {last} <{email}> type={email_type}")


# ── Candidate selection ───────────────────────────────────────────────────────

def select_candidates(rows, exclude_emails=None):
    exclude_emails = exclude_emails or set()
    today  = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=FIRM_LOCKOUT_DAYS)

    locked = set()
    for row in rows:
        if row.get("Status", "").strip() == "sent":
            try:
                if datetime.strptime(row.get("Date Sent", ""), "%Y-%m-%d").date() > cutoff:
                    locked.add(row.get("Email", "").split("@")[-1].lower())
            except ValueError:
                pass

    pending = [
        r for r in rows
        if r.get("Status", "").strip() == "pending"
        and r.get("Email", "").split("@")[-1].lower() not in locked
        and r.get("Email", "").strip().lower() not in exclude_emails
    ]
    logger.info(f"Pending: {len(pending)} | Locked domains: {len(locked)} | Excluded: {len(exclude_emails)}")
    if not pending:
        return []
    return random.sample(pending, min(BATCH_SIZE, len(pending)))


# ── Main ──────────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    if not BRAVE_API_KEY:
        logger.error("BRAVE_API_KEY not set")
        return {"statusCode": 500, "error": "BRAVE_API_KEY missing"}

    rows, fieldnames = load_csv(OUTREACH_BUCKET, OUTREACH_KEY)
    logger.info(f"Queue: {len(rows)} rows")

    # Active deals: load once at startup so each draft can be informed by
    # what Chad is currently active on. Soft-fail to empty list.
    active_deals_summary = load_active_deals_summary()

    # Market context fetched ONCE per invocation; shared across all drafts so
    # the middle paragraph in each email is grounded in the same source material
    # but framed differently.
    market_context = fetch_market_context()

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    winners_sent          = []
    all_held              = []
    all_skipped           = []
    locked_in_run         = set()   # firm domains already used by a winner this invocation
    seen_emails           = set()   # candidates already evaluated this invocation
    prior_first_sentences = []      # for cross-draft uniqueness checks

    for batch_num in range(MAX_BATCHES):
        if len(winners_sent) >= TARGET_WINNERS:
            logger.info(f"Reached target of {TARGET_WINNERS} winners")
            break

        candidates = select_candidates(rows, exclude_emails=seen_emails)
        if not candidates:
            logger.info("No more pending candidates")
            break

        for c in candidates:
            seen_emails.add(c.get("Email", "").strip().lower())

        logger.info(f"Batch {batch_num + 1}: {[r.get('Email') for r in candidates]}")

        # Research all in parallel
        research_list = [None] * len(candidates)
        with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
            futures = {executor.submit(research_firm, row): i for i, row in enumerate(candidates)}
            for future in as_completed(futures):
                research_list[futures[future]] = future.result()

        # Evaluate
        evaluations, qualifying = evaluate_candidates(candidates, research_list)

        # Apply hold/skip statuses immediately
        for ev in evaluations:
            i     = ev.get("index", -1)
            etype = ev.get("type", "")
            if not (0 <= i < len(candidates)):
                continue
            email = candidates[i].get("Email", "").strip().lower()
            if etype == "hold":
                all_held.append(email)
                for row in rows:
                    if row.get("Email", "").strip().lower() == email:
                        row["Status"]    = "hold"
                        row["Date Sent"] = today_str
                        break
            elif etype == "skip":
                all_skipped.append(email)
                for row in rows:
                    if row.get("Email", "").strip().lower() == email:
                        row["Status"]    = "skip"
                        row["Date Sent"] = today_str
                        break

        # Process qualifying in priority order, enforcing in-run firm lockout
        for q in qualifying:
            if len(winners_sent) >= TARGET_WINNERS:
                break

            idx           = q["index"]
            winner        = candidates[idx]
            winner_type   = q["type"]
            winner_angle  = q["angle"]
            winner_research = research_list[idx]
            winner_email  = winner.get("Email", "").strip().lower()
            winner_domain = winner_email.split("@")[-1] if "@" in winner_email else ""

            if winner_domain and winner_domain in locked_in_run:
                logger.info(
                    f"Skipping {winner_email}: firm domain {winner_domain} "
                    f"already used by a winner this run"
                )
                continue

            research_summary = summarize_research(
                winner, winner_research, winner_angle,
                market_context_text=market_context_text(market_context),
            )

            draft = draft_email(
                winner, winner_type, winner_angle, winner_research,
                market_context=market_context,
                prior_first_sentences=prior_first_sentences,
                active_deals_summary=active_deals_summary,
            )
            if not draft.get("subject") or not draft.get("body"):
                logger.error(f"Draft generation failed for {winner_email}; skipping")
                continue

            send_digest(
                winner, winner_type, winner_angle, winner_research, draft,
                research_summary=research_summary,
            )

            for row in rows:
                if row.get("Email", "").strip().lower() == winner_email:
                    row["Status"]    = "sent"
                    row["Date Sent"] = today_str
                    break

            fs = _first_sentence(draft.get("body", ""))
            if fs:
                prior_first_sentences.append(fs)

            if winner_domain:
                locked_in_run.add(winner_domain)
            winners_sent.append({
                "prospect": f"{winner.get('First Name')} {winner.get('Last Name')} <{winner_email}>",
                "type":     winner_type,
                "angle":    winner_angle,
            })
            logger.info(
                f"Winner #{len(winners_sent)} batch {batch_num + 1}: "
                f"{winner.get('First Name')} {winner.get('Last Name')} type={winner_type}"
            )

        # Persist after every batch so partial progress survives a crash
        save_csv(OUTREACH_BUCKET, OUTREACH_KEY, rows, fieldnames)
        logger.info(
            f"Batch {batch_num + 1} done. winners_sent={len(winners_sent)} "
            f"held={len(all_held)} skipped={len(all_skipped)}"
        )

    save_csv(OUTREACH_BUCKET, OUTREACH_KEY, rows, fieldnames)

    if not winners_sent:
        logger.info("No qualifying prospect found.")
        return {
            "statusCode": 200,
            "message":    "No qualifying prospect found",
            "held":       all_held,
            "skipped":    all_skipped,
        }

    return {
        "statusCode":     200,
        "winners_sent":   len(winners_sent),
        "target":         TARGET_WINNERS,
        "prospects":      winners_sent,
        "held":           all_held,
        "skipped":        all_skipped,
    }
