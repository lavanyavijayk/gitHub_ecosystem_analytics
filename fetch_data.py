"""
GH Archive Downloader
=====================
Downloads 6 peak hours (14:00-19:00 UTC) for 1 selected day per year
from 2016 to 2025. Total: 60 files, ~2-2.5 GB compressed.

Usage:
    python gh_archive_download.py

Requirements:
    pip install requests tqdm
"""

import requests
import os
import time
import json
import gzip
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================

# One carefully selected mid-week day per year (no holidays, no weekends)
DATES = [
    "2016-03-15",  # Tuesday  - mid-week, no holidays
    "2017-04-12",  # Wednesday - stable period
    "2018-03-14",  # Wednesday - mid-week, no holidays
    "2019-04-10",  # Wednesday - pre-COVID baseline
    "2020-04-15",  # Wednesday - COVID lockdown period
    "2021-03-17",  # Wednesday - post-COVID recovery
    "2022-04-13",  # Wednesday - stable period
    "2023-03-15",  # Wednesday - pre-AI boom
    "2024-04-10",  # Wednesday - AI boom in full swing
    "2025-03-12",  # Wednesday - most recent data
    "2026-04-15", # wednesday
]

# Peak global developer activity hours (UTC)
# Covers US afternoon + European evening
PEAK_HOURS = [14, 15, 16, 17, 18, 19]

# Download directory
DOWNLOAD_DIR = "data/gharchive_raw"

# Event types we care about (for the optional filter step)
RELEVANT_EVENTS = [
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "ForkEvent",
    "CreateEvent",
]


# ============================================================
# DOWNLOAD FUNCTIONS
# ============================================================

def download_file(url, filepath, retries=3):
    """Download a single file with retry logic."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, stream=True, timeout=60)
            if resp.status_code == 200:
                total = int(resp.headers.get("content-length", 0))
                downloaded = 0

                with open(filepath, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                        downloaded += len(chunk)

                size_mb = os.path.getsize(filepath) / (1024 * 1024)
                print(f"  ✓ {os.path.basename(filepath)} ({size_mb:.1f} MB)")
                return True
            elif resp.status_code == 404:
                print(f"  ✗ {os.path.basename(filepath)} - NOT FOUND (may not exist yet)")
                return False
            else:
                print(f"  ✗ {os.path.basename(filepath)} - HTTP {resp.status_code} (attempt {attempt + 1})")
        except requests.exceptions.RequestException as e:
            print(f"  ✗ {os.path.basename(filepath)} - Error: {e} (attempt {attempt + 1})")

        if attempt < retries - 1:
            time.sleep(5)

    print(f"  ✗ FAILED after {retries} attempts: {os.path.basename(filepath)}")
    return False


def download_all():
    """Download all GH Archive files for selected dates and peak hours."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    total_files = len(DATES) * len(PEAK_HOURS)
    downloaded = 0
    failed = 0
    skipped = 0
    total_size = 0

    print("=" * 60)
    print("GH Archive Downloader")
    print("=" * 60)
    print(f"Years:      {DATES[0][:4]} to {DATES[-1][:4]} ({len(DATES)} days)")
    print(f"Hours/day:  {PEAK_HOURS[0]}:00 - {PEAK_HOURS[-1]}:00 UTC ({len(PEAK_HOURS)} hours)")
    print(f"Total files: {total_files}")
    print(f"Output dir:  {DOWNLOAD_DIR}/")
    print("=" * 60)

    for date in DATES:
        year = date[:4]
        year_dir = os.path.join(DOWNLOAD_DIR, year)
        os.makedirs(year_dir, exist_ok=True)

        print(f"\n📅 {date}")

        for hour in PEAK_HOURS:
            filename = f"{date}-{hour}.json.gz"
            filepath = os.path.join(year_dir, filename)
            url = f"https://data.gharchive.org/{date}-{hour}.json.gz"

            # Skip if already downloaded
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                size_mb = os.path.getsize(filepath) / (1024 * 1024)
                print(f"  ⏭ {filename} already exists ({size_mb:.1f} MB)")
                skipped += 1
                total_size += os.path.getsize(filepath)
                continue

            if download_file(url, filepath):
                downloaded += 1
                total_size += os.path.getsize(filepath)
            else:
                failed += 1

            # Small delay to be polite to the server
            time.sleep(1)

    # Summary
    total_size_gb = total_size / (1024 * 1024 * 1024)
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"Downloaded:  {downloaded} files")
    print(f"Skipped:     {skipped} files (already existed)")
    print(f"Failed:      {failed} files")
    print(f"Total size:  {total_size_gb:.2f} GB compressed")
    print("=" * 60)

    return failed == 0


# ============================================================
# FILTER FUNCTION (OPTIONAL - Run after download)
# ============================================================

def filter_events():
    """
    Filter downloaded files to keep only relevant event types.
    This significantly reduces data size before uploading to Azure.

    Creates a new directory 'gharchive_filtered' with cleaned files.
    """
    filtered_dir = "data/gharchive_filtered"
    os.makedirs(filtered_dir, exist_ok=True)

    total_events = 0
    kept_events = 0
    total_raw_size = 0
    total_filtered_size = 0

    print("\n" + "=" * 60)
    print("FILTERING EVENTS")
    print(f"Keeping only: {', '.join(RELEVANT_EVENTS)}")
    print("=" * 60)

    for year_folder in sorted(os.listdir(DOWNLOAD_DIR)):
        year_path = os.path.join(DOWNLOAD_DIR, year_folder)
        if not os.path.isdir(year_path):
            continue

        out_year_dir = os.path.join(filtered_dir, year_folder)
        os.makedirs(out_year_dir, exist_ok=True)

        print(f"\n📅 {year_folder}")

        for filename in sorted(os.listdir(year_path)):
            if not filename.endswith(".json.gz"):
                continue

            input_path = os.path.join(year_path, filename)
            output_path = os.path.join(out_year_dir, filename)
            raw_size = os.path.getsize(input_path)
            total_raw_size += raw_size

            file_total = 0
            file_kept = 0

            with gzip.open(input_path, "rt", encoding="utf-8") as fin, \
                 gzip.open(output_path, "wt", encoding="utf-8") as fout:
                for line in fin:
                    file_total += 1
                    try:
                        event = json.loads(line)
                        if event.get("type") in RELEVANT_EVENTS:
                            # Extract only the fields we need to reduce size
                            cleaned = extract_fields(event)
                            fout.write(json.dumps(cleaned) + "\n")
                            file_kept += 1
                    except json.JSONDecodeError:
                        continue

            filtered_size = os.path.getsize(output_path)
            total_filtered_size += filtered_size
            total_events += file_total
            kept_events += file_kept

            raw_mb = raw_size / (1024 * 1024)
            filtered_mb = filtered_size / (1024 * 1024)
            pct = (file_kept / file_total * 100) if file_total > 0 else 0

            print(f"  ✓ {filename}: {file_total:,} → {file_kept:,} events "
                  f"({pct:.0f}%) | {raw_mb:.1f} MB → {filtered_mb:.1f} MB")

    # Summary
    raw_gb = total_raw_size / (1024 * 1024 * 1024)
    filtered_gb = total_filtered_size / (1024 * 1024 * 1024)
    reduction = (1 - total_filtered_size / total_raw_size) * 100 if total_raw_size > 0 else 0

    print("\n" + "=" * 60)
    print("FILTER SUMMARY")
    print("=" * 60)
    print(f"Total events:    {total_events:,}")
    print(f"Kept events:     {kept_events:,} ({kept_events/total_events*100:.1f}%)")
    print(f"Raw size:        {raw_gb:.2f} GB")
    print(f"Filtered size:   {filtered_gb:.2f} GB")
    print(f"Size reduction:  {reduction:.0f}%")
    print(f"Output dir:      {filtered_dir}/")
    print("=" * 60)


def extract_fields(event):
    """
    Preserve the GH Archive envelope structure expected by silver_layer.py.

    silver_layer.py reads JSON with a schema that expects:
      - actor / repo as nested objects (not flattened)
      - payload as a JSON *string* containing only the fields each event type needs

    Heavy fields (commit lists, diff hunks, etc.) are dropped to reduce file size
    while keeping every field that silver_layer.py actually parses.
    """
    event_type = event.get("type")
    payload    = event.get("payload", {})

    if event_type == "PushEvent":
        slim_payload = {
            "ref":           payload.get("ref"),
            "size":          payload.get("size", 0),
            "distinct_size": payload.get("distinct_size", 0),
        }

    elif event_type == "PullRequestEvent":
        pr = payload.get("pull_request", {})
        slim_payload = {
            "action": payload.get("action"),
            "pull_request": {
                "id":              pr.get("id"),
                "title":           pr.get("title"),
                "state":           pr.get("state"),
                "merged":          pr.get("merged", False),
                "additions":       pr.get("additions"),
                "deletions":       pr.get("deletions"),
                "changed_files":   pr.get("changed_files"),
                "review_comments": pr.get("review_comments"),
                "created_at":      pr.get("created_at"),
                "merged_at":       pr.get("merged_at"),
                "closed_at":       pr.get("closed_at"),
            },
        }

    elif event_type == "IssuesEvent":
        issue = payload.get("issue", {})
        slim_payload = {
            "action": payload.get("action"),
            "issue": {
                "id":         issue.get("id"),
                "title":      issue.get("title"),
                "state":      issue.get("state"),
                "comments":   issue.get("comments"),
                "created_at": issue.get("created_at"),
                "closed_at":  issue.get("closed_at"),
                "labels":     [{"name": lbl.get("name")} for lbl in issue.get("labels", [])],
            },
        }

    elif event_type == "WatchEvent":
        slim_payload = {"action": payload.get("action")}

    elif event_type == "ForkEvent":
        forkee = payload.get("forkee", {})
        slim_payload = {
            "forkee": {
                "id":        forkee.get("id"),
                "full_name": forkee.get("full_name"),
            }
        }

    elif event_type == "CreateEvent":
        slim_payload = {
            "ref_type":    payload.get("ref_type"),
            "ref":         payload.get("ref"),
            "description": payload.get("description"),
        }

    else:
        slim_payload = {}

    return {
        "id":         event.get("id"),
        "type":       event_type,
        "created_at": event.get("created_at"),
        "actor": {
            "id":    event.get("actor", {}).get("id"),
            "login": event.get("actor", {}).get("login"),
        },
        "repo": {
            "id":   event.get("repo", {}).get("id"),
            "name": event.get("repo", {}).get("name"),
        },
        "payload": json.dumps(slim_payload),
    }


# ============================================================
# QUICK STATS (Optional - peek at what you downloaded)
# ============================================================

def peek_at_data():
    """Show a quick summary of what's in the downloaded data."""
    print("\n" + "=" * 60)
    print("DATA PEEK - Checking first file of each year")
    print("=" * 60)

    for year_folder in sorted(os.listdir(DOWNLOAD_DIR)):
        year_path = os.path.join(DOWNLOAD_DIR, year_folder)
        if not os.path.isdir(year_path):
            continue

        files = sorted([f for f in os.listdir(year_path) if f.endswith(".json.gz")])
        if not files:
            continue

        first_file = os.path.join(year_path, files[0])
        event_counts = {}
        total = 0
        sample_repos = set()

        with gzip.open(first_file, "rt", encoding="utf-8") as f:
            for line in f:
                total += 1
                try:
                    event = json.loads(line)
                    etype = event.get("type", "Unknown")
                    event_counts[etype] = event_counts.get(etype, 0) + 1
                    if total <= 10000:
                        sample_repos.add(event.get("repo", {}).get("name"))
                except json.JSONDecodeError:
                    continue

        print(f"\n📅 {year_folder} ({files[0]})")
        print(f"   Total events: {total:,}")
        print(f"   Unique repos (first 10K events): {len(sample_repos):,}")
        print(f"   Event breakdown:")
        for etype, count in sorted(event_counts.items(), key=lambda x: -x[1]):
            pct = count / total * 100
            print(f"      {etype:30s} {count:>8,} ({pct:.1f}%)")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("\n🚀 Starting GH Archive download...\n")

    # Step 1: Download raw files
    success = download_all()

    # Step 2: Peek at the data
    peek_at_data()

    # Step 3: Filter to relevant events only
    print("\n🔍 Starting event filtering...")
    filter_events()

    print("\n✅ All done!")
    print("\nNext steps:")
    print("  1. Upload 'gharchive_filtered/' to Azure Blob Storage")
    print("  2. Run GitHub API enrichment script")
    print("  3. Process in Azure Databricks")