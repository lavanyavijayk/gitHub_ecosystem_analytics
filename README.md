# GitHub OSS Ecosystem Analytics
DAMG 7370 Final Project — End-to-end Azure data pipeline

## Architecture

```
 ┌─────────────────────────────────────────────────────────────┐
 │  MANUAL (one-time)                                          │
 │  python download_gharchive.py                               │
 │  python github_api_enrichment.py --pat <token>              │
 │  python upload_to_blob.py --storage <name> --key <key>      │
 └────────────────────────┬────────────────────────────────────┘
                          │ files land in raw/
                          ▼
 ┌─────────────────────────────────────────────────────────────┐
 │  AUTOMATED (ADF triggers)                                   │
 │                                                             │
 │  Trigger 1: BlobCreated event → fires on every upload       │
 │  Trigger 2: Monthly schedule  → 1st of month, 02:00 UTC     │
 │                                                             │
 │  Pipeline:                                                  │
 │    silver_layer.py  (Raw JSON → Delta silver tables)        │
 │         ↓                                                   │
 │    gold_layer.py    (Silver → Delta gold / star schema)     │
 │         ↓                                                   │
 │    ml_model.py      (Gold → Random Forest predictions)      │
 └────────────────────────┬────────────────────────────────────┘
                          │ Delta gold tables
                          ▼
 ┌─────────────────────────────────────────────────────────────┐
 │  QUERY LAYER                                                │
 │  Synapse Serverless — reads Delta via OPENROWSET            │
 │  Views: gold.dim_* / gold.fact_* / analysis.*              │
 └────────────────────────┬────────────────────────────────────┘
                          │ DirectQuery
                          ▼
 ┌─────────────────────────────────────────────────────────────┐
 │  POWER BI SERVICE (web)                                     │
 │  9 dashboards — shared via workspace link                   │
 └─────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Azure for Students account ($100 credits)
- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.6
- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- Python 3.11+
- Power BI account (free with university email)

---

## Step 1 — Clone & configure

```bash
git clone <your-repo>
cd github-oss-analytics/terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — fill in github_pat and synapse_admin_password
```

---

## Step 2 — Deploy all infrastructure

```bash
az login                    # authenticate to Azure
terraform init              # download providers
terraform plan              # preview what will be created
terraform apply             # build everything (~8 min)
```

Terraform provisions in this order:
1. Resource Group
2. ADLS Gen2 Storage (raw / silver / gold containers)
3. Key Vault + secrets
4. Databricks workspace + notebooks uploaded automatically
5. Synapse Analytics workspace + all SQL views deployed
6. Azure Data Factory + linked services + pipeline + both triggers

Save the outputs — you need `storage_account_name` and `storage_primary_key` for the upload script.

---

## Step 3 — Download data locally

```bash
cd scripts/
pip install requests azure-storage-blob

# Download and filter 60 GH Archive files (~2 GB)
python download_gharchive.py

# Enrich top 2,000 repos via GitHub API (~300 MB)
python github_api_enrichment.py --pat <your_github_pat>
```

---

## Step 4 — Upload to Azure (triggers pipeline automatically)

```bash
python upload_to_blob.py \
  --storage <storage_account_name_from_terraform_output> \
  --key     <storage_primary_key_from_terraform_output>
```

As each file lands in `raw/`, ADF's blob event trigger fires and runs:
`silver_layer → gold_layer → ml_model`

Watch progress in **Azure Data Factory Studio → Monitor**.

---

## Step 5 — Connect Power BI Service (web dashboards)

1. Open [app.powerbi.com](https://app.powerbi.com) with your university account
2. Create a new workspace: **GitHub OSS Analytics**
3. Get Data → Azure → Azure Synapse Analytics (SQL DW)
4. Server: paste `synapse_serverless_endpoint` from Terraform output
5. Database: `master`
6. Authentication: Microsoft Account
7. Import all `analysis.*` views
8. Build 9 report pages (one per dashboard)
9. Publish to workspace → share the workspace URL

---

## Monthly Schedule

The monthly trigger runs automatically on the 1st of every month at 02:00 UTC.
It performs a full refresh — recomputes all silver, gold, and ML tables from scratch.
Power BI dashboards update on next refresh (set to automatic in Power BI Service settings).

---

## Estimated Azure Cost

| Service | Cost |
|---------|------|
| Blob Storage (ADLS Gen2) | ~$1 |
| Databricks (~4 hrs total) | ~$10 |
| Azure Data Factory | ~$3 |
| Synapse Serverless | ~$0.01 |
| Key Vault | ~$0.10 |
| **Total** | **~$14–15** |

---

## Teardown (save credits)

```bash
terraform destroy   # removes ALL resources
```