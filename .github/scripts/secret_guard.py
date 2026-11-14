#!/usr/bin/env python3
"""secret_guard.py - blocks secrets from entering this PUBLIC repo or its PUBLIC logs.

Runs in three places:
  1. Claude Code hook (.claude/settings.json) before every `git commit` / `git push`.
  2. GitHub Actions (.github/workflows/secret-guard.yml) on every push.
  3. By hand:  python3 .github/scripts/secret_guard.py --tree

Checks:
  A. Added lines must not contain secret-looking values (keys, tokens, passwords,
     key-bearing URLs, private keys, AWS/Anthropic/GitHub tokens).
  B. Every `aws lambda ...` call in a workflow must suppress its output
     (`> /dev/null`, `--query`, or `--output text`). Lambda API responses include
     ALL environment variables, and Actions logs on public repos are public.
  C. Workflows must not dump the environment (printenv, env, set -x).
  D. os.environ.get / os.getenv must not carry a non-empty string-literal default for a
     variable whose name contains KEY, SECRET, TOKEN or PASSWORD (a "fallback" value is
     a hardcoded secret). Use "" and fail soft instead.

CI runs --tree on every push and pull request, so a secret already in the tree fails
the build even when the change being pushed did not touch it.

Exit 0 = clean, exit 2 = blocked (Claude Code treats exit 2 as "stop and fix").
A line can be allowed on purpose by adding the comment:  secret-guard: allow
"""
import json
import os
import re
import subprocess
import sys

SECRET_PATTERNS = [
    ("AWS access key", re.compile(r"AKIA[0-9A-Z]{16}")),
    ("Anthropic API key", re.compile(r"sk-ant-[A-Za-z0-9_\-]{20,}")),
    ("GitHub token", re.compile(r"gh[pousr]_[A-Za-z0-9]{30,}")),
    ("Private key", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("Key in URL", re.compile(r"(?i)[?&](api_key|app_key|apikey|key|token|secret|sso|stoken)=[A-Za-z0-9_\-%\*]{16,}")),
    ("Hardcoded secret", re.compile(
        r"(?i)\b[A-Za-z0-9_]*(api_?key|app_?key|secret|token|passw(or)?d|admin_?key|hmac)[A-Za-z0-9_]*"
        r"[\"']?\s*[:=]\s*[\"']([^\"'\s]{12,})[\"']")),
]
# values that look like names of env vars / placeholders, not secrets
BENIGN_VALUE = re.compile(
    r"^[A-Z0-9_]+$"                      # an env var NAME, e.g. "SALARY_BOT_TOKEN"
    r"|^[a-z][a-z\-]*$"                  # plain words like "pipeline-token" (real keys have digits/mixed case)
    r"|^https?://"                       # URLs are checked by the "Key in URL" rule instead
    r"|\.(json|txt|csv|html|py|yml)$"    # file / S3 object names
    r"|^(?i:your|example|placeholder|changeme|xxx|dummy|test|fake)")
# D: environ.get / getenv called with a sensitive NAME and a non-empty literal default
#    (positional, default=..., or a trailing `or "<literal>"`) -> flagged.
ENV_DEFAULT = re.compile(
    r"""(?:os\.environ\.get|os\.getenv)\(\s*(?P<q>["'])(?P<name>[^"']+)(?P=q)\s*"""
    r"""(?:,\s*(?:default\s*=\s*)?[rbuRBU]?(?P<dq>["'])(?P<dval>[^"']*)(?P=dq)"""
    r"""|\)\s*or\s*[rbuRBU]?(?P<oq>["'])(?P<oval>[^"']*)(?P=oq))""")
SENSITIVE_NAME = re.compile(r"(?i)KEY|SECRET|TOKEN|PASSWORD")
SAFE_LINE = re.compile(r"os\.environ|getenv|process\.env|secrets\.|\$\{\{|secret-guard: allow")
SKIP_FILES = re.compile(r"(^|/)(package-lock\.json|.*\.lock|.*\.min\.js|.*\.svg|.*\.png|.*\.jpg|.*\.pdf)$")
LAMBDA_CALL = re.compile(r"\baws\s+lambda\s+")
LAMBDA_SAFE = re.compile(r">\s*/dev/null|--query|--output\s+text|--no-cli-pager.*>")
ENV_DUMP = re.compile(r"^\s*(-\s*run:\s*)?(printenv|env|set\s+-x)\s*$|\bprintenv\b")


def sh(*args):
    return subprocess.run(args, capture_output=True, text=True).stdout


def env_default_hits(text):
    """Names of sensitive env vars given a non-empty literal default in `text`."""
    out = []
    for m in ENV_DEFAULT.finditer(text):
        val = m.group("dval") if m.group("dq") else m.group("oval")
        if val and SENSITIVE_NAME.search(m.group("name")):
            out.append(m.group("name"))
    return out


def scan_line(path, line):
    if SKIP_FILES.search(path) or "secret-guard: allow" in line:
        return []
    hits = ["Hardcoded default for env var " + n for n in env_default_hits(line)]
    if SAFE_LINE.search(line):
        return hits
    for name, pat in SECRET_PATTERNS:
        m = pat.search(line)
        if not m:
            continue
        if name == "Hardcoded secret" and BENIGN_VALUE.search(m.group(m.lastindex)):
            continue
        hits.append(name)
    return hits


def added_lines(diff_text):
    """Yield (path, line) for each added line in a unified diff."""
    path = None
    for raw in diff_text.splitlines():
        if raw.startswith("+++ "):
            path = raw[6:] if raw.startswith("+++ b/") else raw[4:]
        elif raw.startswith("+") and not raw.startswith("+++") and path:
            yield path, raw[1:]


def check_workflows(root="."):
    problems = []
    wf_dir = os.path.join(root, ".github", "workflows")
    if not os.path.isdir(wf_dir):
        return problems
    for fn in sorted(os.listdir(wf_dir)):
        if not fn.endswith((".yml", ".yaml")):
            continue
        p = os.path.join(wf_dir, fn)
        text = open(p, encoding="utf-8", errors="ignore").read()
        joined = re.sub(r"\\\n\s*", " ", text)  # join shell line continuations
        for n, line in enumerate(joined.splitlines(), 1):
            if LAMBDA_CALL.search(line) and not LAMBDA_SAFE.search(line) and "secret-guard: allow" not in line:
                problems.append(f".github/workflows/{fn}: `aws lambda` output not suppressed "
                                f"(add `> /dev/null`) - it prints every Lambda secret into the PUBLIC log")
            if ENV_DUMP.search(line) and "secret-guard: allow" not in line:
                problems.append(f".github/workflows/{fn}: environment dump (`{line.strip()}`) would publish secrets")
    return problems


def report(findings, problems):
    if not findings and not problems:
        return 0
    out = ["SECRET GUARD BLOCKED THIS. This repo and its Actions logs are PUBLIC.", ""]
    # Never echo the offending text: in CI this output lands in a PUBLIC log.
    seen = set()
    for path, name, line in findings:
        var = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*[:=]", line)
        where = f"{path}: {name}" + (f" (variable {var.group(1)})" if var else "")
        if where not in seen:
            seen.add(where)
            out.append(f"- {where}")
    out += [f"- {p}" for p in problems]
    out += ["", "Fix: put the value in a Lambda environment variable (os.environ) or s3://pipeline-token/,",
            "never in code, docs, tests, commit messages or logs. Do NOT bypass this check."]
    sys.stderr.write("\n".join(out) + "\n")
    return 2


def scan_diff(diff_text):
    return [(p, name, l) for p, l in added_lines(diff_text) for name in scan_line(p, l)]


def main():
    args = sys.argv[1:]
    mode = args[0] if args else "--hook"
    if mode == "--hook":
        # Claude Code PreToolUse hook: only act on git commit / git push
        try:
            cmd = json.load(sys.stdin).get("tool_input", {}).get("command", "")
        except Exception:
            return 0
        if not re.search(r"\bgit\b[^|;&]*\b(commit|push)\b", cmd):
            return 0
        msg = " ".join(re.findall(r"-m\s+(\"[^\"]*\"|'[^']*')", cmd))
        diff = sh("git", "diff", "--cached", "-U0")
        if " push" in cmd or "push " in cmd:
            up = sh("git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}").strip() or "origin/main"
            diff += sh("git", "diff", "-U0", f"{up}...HEAD")
        findings = scan_diff(diff) + [("commit message", n, msg) for n in scan_line("msg", msg)]
        return report(findings, check_workflows())
    if mode == "--range":
        rng = args[1]
        diff = sh("git", "diff", "-U0", rng)
        msgs = sh("git", "log", "--format=%B", rng.replace("...", ".."))
        findings = scan_diff(diff) + [("commit message", n, m) for m in msgs.splitlines() for n in scan_line("msg", m)]
        return report(findings, check_workflows())
    if mode == "--tree":
        findings = []
        for path in sh("git", "ls-files").splitlines():
            if SKIP_FILES.search(path) or not os.path.isfile(path):
                continue
            try:
                text = open(path, encoding="utf-8", errors="strict").read()
            except (UnicodeDecodeError, OSError):
                continue
            line_hits = set()
            for line in text.splitlines():
                for name in scan_line(path, line):
                    line_hits.add(name)
                    findings.append((path, name, line))
            # D again on the whole file, so a call split across lines is caught too
            for n in env_default_hits(text):
                name = "Hardcoded default for env var " + n
                if name not in line_hits:
                    findings.append((path, name, ""))
        return report(findings, check_workflows())
    print(__doc__)
    return 0


if __name__ == "__main__":
    sys.exit(main())
