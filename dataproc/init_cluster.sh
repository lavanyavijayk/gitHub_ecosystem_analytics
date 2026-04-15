#!/bin/bash
# ================================================================
# init_cluster.sh  —  Dataproc cluster initialization script
#
# Installs Python packages and Delta Lake JARs required by PySpark jobs.
# Runs on ALL nodes (master + workers) at cluster creation time.
#
# Notes:
#   - Delta JARs are downloaded here (not via spark.jars.packages)
#     to avoid transient Maven Central download failures at job time.
#   - Timeout in the workflow is set to 600s to allow for slow downloads.
# ================================================================

set -euxo pipefail

# ── Python packages ──────────────────────────────────────────────
pip install --quiet --upgrade \
  delta-spark==2.3.0 \
  google-cloud-secret-manager \
  google-cloud-storage

# ── Delta Lake JARs → Spark jars directory ───────────────────────
# Pre-download so spark.jars.packages is not needed at job submit time.
SPARK_JARS_DIR="/usr/lib/spark/jars"
DELTA_VERSION="2.3.0"
MAVEN_BASE="https://repo1.maven.org/maven2"

for attempt in 1 2 3; do
  echo "[init] Downloading Delta Lake JARs (attempt ${attempt})..."
  wget -q -P "${SPARK_JARS_DIR}" \
    "${MAVEN_BASE}/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar" \
    "${MAVEN_BASE}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    && echo "[init] Delta JARs installed successfully." && break
  echo "[init] Download failed, retrying in 10s..."
  sleep 10
done

# Verify the JARs exist
ls -la "${SPARK_JARS_DIR}"/delta-*.jar
