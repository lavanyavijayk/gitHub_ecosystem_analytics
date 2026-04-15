# github_api_enrichment.py
#
# PURPOSE: Identify top 2,000 repos from downloaded events
#          and enrich them via the GitHub REST API.
# USAGE:   python github_api_enrichment.py --pat ghp_xxxx
# ================================================================

import argparse, gzip, json, time
from pathlib import Path
from collections import Counter
import requests

OUTPUT_DIR = Path("data/raw/github_api")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RAW_DIR    = Path("data/raw/gharchive")
TOP_N      = 2000
RATE_LIMIT = 5000   # GitHub free tier per hour


def get_top_repos(n: int) -> list[str]:
    """Count repo appearances across all downloaded event files."""
    counts: Counter = Counter()
    for f in RAW_DIR.glob("*.gz"):
        with gzip.open(f, "rt", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                try:
                    evt = json.loads(line)
                    repo = evt.get("repo", {}).get("name")
                    if repo:
                        counts[repo] += 1
                except json.JSONDecodeError:
                    continue
    return [name for name, _ in counts.most_common(n)]


def check_has_readme(name: str, session: requests.Session) -> bool:
    """Check if the repo has a README via the GitHub contents API."""
    url = f"https://api.github.com/repos/{name}/readme"
    try:
        resp = session.get(url, timeout=15)
        return resp.status_code == 200
    except Exception:
        return False


def fetch_repo(name: str, session: requests.Session) -> dict | None:
    """Fetch metadata for one repo via GitHub REST API."""
    url  = f"https://api.github.com/repos/{name}"
    resp = session.get(url, timeout=30)

    if resp.status_code == 404:
        return None   # repo deleted or renamed
    if resp.status_code == 403:
        # Rate limit hit — wait and retry once
        reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
        wait  = max(0, reset - time.time()) + 5
        print(f"  [rate_limit] sleeping {wait:.0f}s")
        time.sleep(wait)
        resp = session.get(url, timeout=30)

    resp.raise_for_status()
    data = resp.json()

    return {
        "repo_id":          data["id"],
        "repo_name":        data["full_name"],
        "language":         data.get("language"),
        "license":          (data.get("license") or {}).get("spdx_id"),
        "topics":           ",".join(data.get("topics", [])),
        "stars_at_extract": data["stargazers_count"],
        "forks_at_extract": data["forks_count"],
        "description":      data.get("description", "")[:500],
        "is_archived":      data["archived"],
        "has_readme":       check_has_readme(name, session),
        "has_ci":           ".github/workflows" in data.get("contents_url", ""),
        "created_at":       data["created_at"],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pat", required=True, help="GitHub Personal Access Token")
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({
        "Authorization": f"token {args.pat}",
        "Accept":        "application/vnd.github.v3+json",
    })

    print(f"Finding top {TOP_N} repos...")
    top_repos = get_top_repos(TOP_N)
    print(f"Found {len(top_repos)} repos")

    results = []
    for i, name in enumerate(top_repos):
        try:
            data = fetch_repo(name, session)
            if data:
                results.append(data)
        except Exception as e:
            print(f"  [error] {name}: {e}")

        # Polite rate limiting — stay well under 5,000/hr
        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{len(top_repos)}] fetched so far...")
            time.sleep(2)

    out_path = OUTPUT_DIR / "repositories.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nSaved {len(results)} repos → {out_path}")


if __name__ == "__main__":
    main()