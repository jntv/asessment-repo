# UHC Transparency-in-Coverage Data Pipeline

End-to-end ingest of UHC in-network rate files: download 1000 files, parse to parquet, load to Snowflake, and extract insights.

## Setup

### 1. Python Environment

```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### 2. Snowflake Account

1. Sign up at [signup.snowflake.com](https://signup.snowflake.com)
2. Choose **AWS**, **US East (N. Virginia)**, **Enterprise** edition
3. After signup, note your account identifier (e.g., `abc12345.us-east-1`)

### 3. Configure `.env`

Copy `.env.example` to `.env` and fill in your Snowflake credentials:

```bash
cp .env.example .env
# Edit .env with your details
```

### 4. Optional: AWS S3 Setup (if using STORAGE_MODE=S3)

**DEFAULT: Uses LOCAL storage (parquet files saved locally, then PUTted to Snowflake internal stage).**

To enable cloud storage mode (parquet → S3 → Snowflake external stage):

#### 4a. Create AWS Resources

1. **Create an S3 bucket:**
   - Console: https://s3.console.aws.amazon.com/
   - Name: e.g., `uhc-parquet-data-<your-name>`
   - Region: Choose same as your Snowflake account (e.g., us-east-1)
   - Accept defaults

2. **Create an IAM role for Snowflake:**
   - Console: https://iam.aws.amazon.com/
   - Role name: `snowflake-s3-role`
   - Trust relationship (Trusted entity): Choose **AWS Account** → paste your Snowflake AWS account ID (from Snowflake `DESCRIBE STORAGE INTEGRATION s3_int`)
   - Permissions policy: Create inline policy with:
     ```json
     {
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Action": [
             "s3:GetObject",
             "s3:PutObject",
             "s3:ListBucket"
           ],
           "Resource": [
             "arn:aws:s3:::uhc-parquet-data-<your-name>/*",
             "arn:aws:s3:::uhc-parquet-data-<your-name>"
           ]
         }
       ]
     }
     ```
   - Copy the role ARN: `arn:aws:iam::YOUR_ACCOUNT_ID:role/snowflake-s3-role`

#### 4b. Configure Python

Update `.env`:

```env
STORAGE_MODE=S3
AWS_ACCESS_KEY_ID=<your-access-key>
AWS_SECRET_ACCESS_KEY=<your-secret-key>
AWS_REGION=us-east-1
S3_BUCKET=uhc-parquet-data-<your-name>
S3_PREFIX=parquet/
SF_S3_ROLE=arn:aws:iam::YOUR_ACCOUNT_ID:role/snowflake-s3-role
```

#### 4c. Configure Snowflake

In Snowflake SQL editor, run:

```sql
-- Replace YOUR_AWS_ACCOUNT_ID and bucket name below
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/snowflake-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://uhc-parquet-data-<your-name>/parquet/');

CREATE OR REPLACE STAGE s3_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://uhc-parquet-data-<your-name>/parquet/'
  FILE_FORMAT = (TYPE = PARQUET);
```

### 5. Create Snowflake Schema

Log into Snowflake web UI, open a SQL worksheet, and run:

```sql
-- From sql/01_ddl.sql (entire file, or just the first part for LOCAL mode)
CREATE OR REPLACE DATABASE uhc_tic;
CREATE OR REPLACE SCHEMA uhc_tic.staging;
CREATE OR REPLACE SCHEMA uhc_tic.analytics;
-- ... (rest of DDL from the file)
```

If using **S3 mode**, also uncomment and run the S3 stage section at the bottom of `sql/01_ddl.sql` (fill in your values).

## Storage Options

Choose **LOCAL** (default) or **S3** by setting `STORAGE_MODE` in `.env`:

- **LOCAL**: Parse to `data/parquet/`, upload to Snowflake via `PUT`. Good for development.
- **S3**: Parse to `data/parquet/`, upload to S3, load from S3 to Snowflake. Good for production/cloud workflows.

To switch modes, just change `STORAGE_MODE` in `.env` and re-run `parse` and `load` steps.

## Workflow

### Prerequisites (One-Time Setup)

```bash
# Activate environment
.\.venv\Scripts\Activate.ps1

# Get the index file (~500 MB)
python src/download.py get-index

# Extract manifest (first 1000 plan URLs)
python src/download.py manifest

# Fetch file sizes and sort by size (smallest first)
python src/download.py sizes
```

This creates:
- `files/index_file/manifest_sized.csv` — All 1000 files with sizes, sorted smallest first

### Incremental File-by-File Processing

Process files one at a time. For each file (starting with row 1, the smallest):

**Step 1: Download**

```bash
python src/download.py download-one 1
```

Downloads file #1 from `manifest_sized.csv`, unzips it, and deletes the `.gz`. Returns the path to the `.json` file.

**Step 2: Parse to Parquet + Upload to S3**

```bash
python src/parse.py one 1
```

Parses the JSON file to parquet, uploads to S3, deletes local files. Logs status.

**Step 3: Load to Snowflake**

```bash
python src/load_snowflake.py
```

Creates S3 stage (if needed), then COPY INTOs the table from S3. Runs incrementally — each parse automatically loads to Snowflake on next run.

**Repeat for Each File**

For file #2:

```bash
python src/download.py download-one 2
python src/parse.py one 2
python src/load_snowflake.py
```

For file #3, #4, etc., increment the row number.

---

### Alternative: Batch Processing (All Files at Once)

If you want to download and process all 1000 files in parallel (not recommended — takes 20+ hours and lots of disk):

```bash
# Download all files (4 worker threads, ~8 hours)
python src/download.py download

# Parse all to parquet (6-12 hours)
python src/parse.py all

# Load all to Snowflake (1-2 hours)
python src/load_snowflake.py
```

---

### Troubleshooting the Incremental Workflow

**Q: Can I stop mid-way?**  
A: Yes. The download deletes the `.gz` after unzipping. Parse deletes the `.json` after uploading to S3. You can resume with the next row.

**Q: What if a download fails?**  
A: The script logs errors to `files/index_file/download_log.csv`. Just re-run the same row.

**Q: What if parse fails?**  
A: The local JSON remains in `files/network_files/`. Fix the error and re-run `parse.py one <row>`.

**Q: Can I load to Snowflake after every file, or batch load at the end?**  
A: Either works. `load_snowflake.py` appends to the table, so you can run it after each parse or once at the end.

### Example: Process First 5 Files

```bash
# One-time setup (first time only)
python src/download.py get-index
python src/download.py manifest
python src/download.py sizes

# Then loop through files 1-5
for $i in 1..5 {
    python src/download.py download-one $i
    python src/parse.py one $i
    python src/load_snowflake.py
    Write-Host "Completed file $i"
}
```

### Step 7: Sanity Check & Analysis

After loading some files, inspect in a Jupyter notebook:

```python
import pandas as pd
df = pd.read_parquet("data/parquet/sample.parquet")
df.shape
df["billing_code_type"].value_counts()
df["negotiated_rate"].describe()
```

## Design Decisions

**Why stream-parse instead of load everything?** — The raw files total ~300 GB compressed, 3–8 TB uncompressed. Streaming with ijson keeps RAM constant. We parse to parquet (which represents only the columns we care about) to ~10 GB total.

**Why sort by file size?** — Lets us download smallest files first, fail fast on any network issues, and decide mid-run whether to keep the 30+ GB outliers.

**Why delete raw files after parsing?** — Parquet is ~1/30th the size of the raw gz. Keeps disk usage manageable.

**Why ijson over PySpark?** — Simpler dependency chain, no JVM overhead, easier to debug. PySpark's nested JSON handling is awkward for this deeply nested structure. Save Spark for aggregations *in* Snowflake.

**Why add S3 support?** — For production workflows, cloud storage decouples data processing from compute. Demonstrates cloud-native architecture. Default to LOCAL for simplicity; set `STORAGE_MODE=S3` for S3.

## Insights to Explore

See `sql/` for analysis queries. Plan to generate 3–4 insights with charts:

- Price variation for the same procedure (CPT code variation across providers)
- Negotiation arrangement mix (FFS vs bundle vs capitation)
- Plan-level concentration (distinct codes and providers per plan)
- Outliers and data quality flags

Add-on data to consider:
- **CMS NPPES NPI Registry** — turn NPI into provider name/specialty/ZIP
- **CMS HCPCS/CPT descriptors** — fill gaps in service descriptions

## What's Next

- [ ] Download and parse all 1000 files
- [ ] Load to Snowflake, sanity-check row counts
- [ ] Write 3–4 analysis queries
- [ ] Create charts (Snowsight or Jupyter)
- [ ] Document insights and walkthrough
