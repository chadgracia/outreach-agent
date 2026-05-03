"""
outreach_draft_lambda.py  v3

Flow:
  1. Pick 5 random pending (respecting 7-day firm lockout)
  2. Research all 5 in parallel — fetch firm website via Brave-found URLs
  3. Bedrock evaluates each and assigns: firm_fit | owner_opportunistic | staff_fit | hold | skip
  4. Pick best winner (firm_fit > staff_fit > owner_opportunistic)
  5. Draft email matched to type
  6. Send digest to Chad, update statuses in S3, loop to next batch if no winner

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
MAIN_URL          = "https://trades.graciagroup.com"
BRAVE_API_KEY     = os.environ.get("BRAVE_API_KEY", "")
FIRM_LOCKOUT_DAYS = 7
BATCH_SIZE        = 5
MAX_BATCHES       = 5

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


# ── Web helpers ───────────────────────────────────────────────────────────────

def fetch_page(url, max_chars=3000):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            raw  = resp.read().decode("utf-8", errors="ignore")
        text = re.sub(r"<[^>]+>", " ", raw)
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
                "title":   item.get("title", ""),
                "snippet": item.get("description", ""),
                "url":     item.get("url", ""),
            })
        logger.info(f"Brave '{query[:60]}': {len(results)} results")
        return results
    except Exception as e:
        logger.warning(f"Brave failed: {e}")
        return []


def best_url_from_results(domain, results):
    """Pick the most informative URL Brave found for this domain."""
    USEFUL = ["about", "invest", "portfolio", "focus", "strategy",
              "approach", "companies", "holdings", "team", "what-we-do"]
    # Prefer pages from their own domain with useful keywords
    for kw in USEFUL:
        for r in results:
            u = r.get("url", "").lower()
            if domain in u and kw in u:
                return r["url"]
    # Fall back to any result from their domain
    for r in results:
        if domain in r.get("url", "").lower():
            return r["url"]
    return None


def research_firm(row):
    email  = row.get("Email", "").strip()
    domain = email.split("@")[-1].lower() if "@" in email else ""
    first  = row.get("First Name", "").strip()
    last   = row.get("Last Name", "").strip()
    firm   = row.get("Firm", "").strip()

    # Brave search for firm
    results = brave_search(f'"{domain}" OR "{firm}" family office investments', count=6)

    # Fetch homepage
    homepage = fetch_page(f"https://{domain}") if domain else ""

    # Fetch best secondary page if different from homepage
    best_url      = best_url_from_results(domain, results)
    secondary     = fetch_page(best_url) if best_url and best_url != f"https://{domain}" else ""

    snippets = "\n".join(f"- {r['title']}: {r['snippet']}" for r in results)

    # Combine: prefer secondary page, fall back to homepage, fall back to snippets
    web_content = secondary or homepage or snippets

    logger.info(
        f"Research {first} {last} @ {domain}: "
        f"homepage={len(homepage)}c, secondary={len(secondary)}c, snippets={len(snippets)}c"
    )
    return {
        "web_content": web_content,
        "snippets":    snippets,
        "domain":      domain,
        "firm":        firm,
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
        context = (res["web_content"] or res["snippets"] or "No content found.")[:600]
        profiles += f"""
--- Candidate {i} ---
Name:    {row.get('First Name','')} {row.get('Last Name','')}
Title:   {row.get('Job Title','')}
Firm:    {res['firm']} ({res['domain']})
Content: {context}
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

Key reasoning rules:
- Family offices rarely publish their holdings. Infer mandate from: firm description, sector 
  language, deal types mentioned, investment philosophy, geography.
- A control PE firm owner = owner_opportunistic (not skip)
- A real estate firm owner = skip (real estate is the business, not a family office wrapper)
- Junior staff (Analyst, Associate) at a family office = firm_fit if the firm qualifies
- Junior staff at a non-qualifying firm = hold or skip depending on firm type
- When in doubt between hold and skip, choose hold

Candidates:
{profiles}

Respond ONLY with valid JSON:
{{
  "evaluations": [
    {{
      "index": 0,
      "type": "firm_fit|owner_opportunistic|hold|skip",
      "angle": "<one sentence — what specifically makes them fit or why they are hold/skip>"
    }},
    ...
  ]
}}"""

    result = bedrock_json(prompt, max_tokens=700)
    evaluations = result.get("evaluations", [])

    # Sort qualifying candidates by priority
    qualifying = [
        e for e in evaluations
        if e.get("type") in TYPE_PRIORITY
    ]
    qualifying.sort(key=lambda e: TYPE_PRIORITY.get(e.get("type"), 99))
    return evaluations, qualifying


# ── Email drafting ────────────────────────────────────────────────────────────

def draft_email(row, email_type, angle, research):
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

    prompt = f"""Write a cold outreach email from Chad Gracia to {first} {last}, {title} at {firm}.

WHAT CHAD DOES: He specializes in secondary transactions in pre-IPO private tech companies like 
Anthropic, SpaceX, Anduril and xAI. He also sometimes has early access to seed rounds before 
they become well known. Trading page: {MAIN_URL}

WHY THIS PERSON (use to inform tone only, do NOT quote back):
{angle}

FIRM RESEARCH (use to inform tone only, do NOT quote back):
{(research['web_content'] or research['snippets'])[:500]}

EMAIL TYPE INSTRUCTIONS:
{type_instruction}

WRITE EXACTLY 3 SENTENCES plus a sign-off. Nothing more.

Sentence 1: Natural context-setter. Brief. Human.
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
Sentence 3: Soft close with trading page link.
Sign-off: Sincerely,

HARD RULES — violation means rejected:
- NO dashes of any kind used as punctuation (no em-dash, no en-dash, no " - " between clauses)
- NO phone call offers, NO "schedule a call", NO "hop on a call", NO "would love to connect"
- NO "excited", "thrilled", "leverage", "synergy", "reach out", "touch base", "circle back"
- NO parroting their website language back at them
- NO deal URLs or specific deal mentions
- Do NOT mention Rainmaker Securities in the body
- Do NOT say "FINRA broker"
- Sound like a smart human wrote it quickly, not a marketing department

Respond ONLY with valid JSON:
{{
  "subject": "<short subject, not salesy>",
  "body": "<3 sentences plus sign-off, plain text, newlines as \\n>"
}}"""

    return bedrock_json(prompt, max_tokens=400)


# ── SES ───────────────────────────────────────────────────────────────────────

def send_digest(row, email_type, angle, research, draft):
    ses   = boto3.client("ses", region_name="us-east-1")
    first = row.get("First Name", "")
    last  = row.get("Last Name", "")
    firm  = row.get("Firm", "")
    email = row.get("Email", "")

    body = f"""OUTREACH DRAFT — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}

PROSPECT
  Name:       {first} {last}
  Title:      {row.get('Job Title', '')}
  Firm:       {firm}
  Email:      {email}
  Type:       {email_type}
  Why:        {angle}

RESEARCH
{(research['web_content'] or research['snippets'])[:600]}

{'=' * 50}
DRAFT EMAIL

{email}
{draft.get('subject', '(none)')}

{draft.get('body', '(no body)')}
{'=' * 50}

To update: email agent@ with "mark {email} as responded" or "mark {email} as skip"
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

def select_candidates(rows):
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
    ]
    logger.info(f"Pending: {len(pending)} | Locked domains: {len(locked)}")
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

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    winner = winner_type = winner_angle = winner_research = None
    all_held    = []
    all_skipped = []

    for batch_num in range(MAX_BATCHES):
        candidates = select_candidates(rows)
        if not candidates:
            logger.info("No more pending candidates")
            break

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
            i    = ev.get("index", -1)
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

        # Pick winner from qualifying (already sorted by priority)
        if qualifying:
            best   = qualifying[0]
            idx    = best["index"]
            winner         = candidates[idx]
            winner_type    = best["type"]
            winner_angle   = best["angle"]
            winner_research = research_list[idx]
            logger.info(
                f"Winner batch {batch_num + 1}: "
                f"{winner.get('First Name')} {winner.get('Last Name')} "
                f"type={winner_type}"
            )
            break

        logger.info(f"Batch {batch_num + 1}: no winner. held={len(all_held)} skipped={len(all_skipped)}")
        save_csv(OUTREACH_BUCKET, OUTREACH_KEY, rows, fieldnames)

    if not winner:
        save_csv(OUTREACH_BUCKET, OUTREACH_KEY, rows, fieldnames)
        logger.info("No qualifying prospect found.")
        return {
            "statusCode": 200,
            "message":    "No qualifying prospect found",
            "held":       all_held,
            "skipped":    all_skipped,
        }

    # Draft email
    draft = draft_email(winner, winner_type, winner_angle, winner_research)
    if not draft.get("subject") or not draft.get("body"):
        logger.error("Draft generation failed")
        return {"statusCode": 500, "error": "Draft generation failed"}

    # Send to Chad
    send_digest(winner, winner_type, winner_angle, winner_research, draft)

    # Mark winner as sent
    winner_email = winner.get("Email", "").strip().lower()
    for row in rows:
        if row.get("Email", "").strip().lower() == winner_email:
            row["Status"]    = "sent"
            row["Date Sent"] = today_str
            break

    save_csv(OUTREACH_BUCKET, OUTREACH_KEY, rows, fieldnames)
    logger.info(f"Marked {winner_email} as sent. held={len(all_held)} skipped={len(all_skipped)}")

    return {
        "statusCode":  200,
        "prospect":    f"{winner.get('First Name')} {winner.get('Last Name')} <{winner_email}>",
        "type":        winner_type,
        "angle":       winner_angle,
        "held":        all_held,
        "skipped":     all_skipped,
    }
