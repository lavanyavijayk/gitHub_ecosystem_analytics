# ================================================================
# scripts/upload_to_gcs.py
#
# PURPOSE: Upload all local data/ files to the GCS raw/ bucket.
#          Run this after fetch_data.py.
#          The Eventarc trigger fires automatically for each file
#          uploaded, launching the incremental Dataproc pipeline.
#
# USAGE:
#   python scripts/upload_to_gcs.py --bucket <prefix>-raw
#
# REQUIRES:
#   pip install google-cloud-storage
#   gcloud auth application-default login  (or set GOOGLE_APPLICATION_CREDENTIALS)
# ================================================================

import argparse
from pathlib import Path

from google.cloud import storage

# Files are uploaded to:  gs://{bucket}/gharchive/{YYYY}/{filename}.json.gz
# This matches the path expected by silver_layer.py (full refresh lists gharchive/**)
DATA_DIR = Path(__file__).parent.parent / "data" / "gharchive_filtered" / "2019"


def upload_folder(client: storage.Client,
                  bucket_name: str,
                  local_dir: Path) -> int:
    """Recursively upload all .gz files to gs://{bucket}/gharchive/..."""
    bucket   = client.bucket(bucket_name)
    uploaded = 0

    for file_path in sorted(local_dir.rglob("*.gz")):
        # Preserve year subdirectory: gharchive/2016/2016-01-01-12.json.gz
        rel       = file_path.relative_to(local_dir)
        blob_name = f"gharchive/{rel}".replace("\\", "/")
        blob      = bucket.blob(blob_name)

        # Skip already-uploaded files (idempotent)
        if blob.exists():
            print(f"  [skip]   {blob_name}")
            continue

        print(f"  [upload] {file_path.name} → gs://{bucket_name}/{blob_name}")
        blob.upload_from_filename(str(file_path))
        uploaded += 1

    return uploaded


def main():
    parser = argparse.ArgumentParser(
        description="Upload GH Archive filtered files to GCS raw bucket.")
    parser.add_argument(
        "--bucket", required=True,
        help="GCS bucket name (e.g. ghoss-raw). "
             "From: terraform output raw_bucket")
    parser.add_argument(
        "--project", default=None,
        help="GCP project ID (optional — uses ADC default if omitted)")
    args = parser.parse_args()

    if not DATA_DIR.exists():
        print(f"ERROR: data directory not found: {DATA_DIR}")
        print("Run fetch_data.py first to download GH Archive files.")
        raise SystemExit(1)

    client = storage.Client(project=args.project)

    print(f"Uploading {DATA_DIR} → gs://{args.bucket}/gharchive/")
    total = upload_folder(client, args.bucket, DATA_DIR)
    print(f"\nUpload complete: {total} new files uploaded to gs://{args.bucket}/gharchive/")
    if total > 0:
        print("Eventarc trigger will fire for each file, "
              "launching the incremental Dataproc pipeline automatically.")
    else:
        print("No new files — all already uploaded.")


if __name__ == "__main__":
    main()
