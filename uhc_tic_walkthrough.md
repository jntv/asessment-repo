# UHC Transparency-in-Coverage assessment — working notes

A walkthrough for the "download UHN index, pull first 1000 files, load into Snowflake, present insights" task. Written as the order I would actually do it in, with the gotchas I hit, the choices I'd make again, and the ones I'd skip.

---

## 0. Before you write a single line of code — understand the data

This is the step most candidates skip, and it's the one that protects you in the walkthrough. Hiring managers ask "why did you do it this way" — you need real answers.

The UHC site (https://transparency-in-coverage.uhc.com) publishes machine-readable files mandated by the CMS Transparency-in-Coverage final rule. There are three file types:

- **Table of Contents (index) file** — a JSON file that points to every in-network rate file and allowed-amount file for every plan UHC issues. This is what "UHN Index file" in the task refers to.
- **In-network rate files** — gzipped JSON, one per plan/issuer combo. These are the heavy ones — single files can be 10–50 GB compressed. The first 1,000 from the index will not all fit on a laptop.
- **Allowed-amount files** — out-of-network claim data. Not what the task asks for, but worth a one-line mention in your walkthrough so the interviewer knows you noticed.

Schema of the in-network rate file (memorize this — the interviewer will probe it):

```
reporting_entity_name, reporting_entity_type, last_updated_on, version
plan_name, plan_id, plan_id_type, plan_market_type
in_network[]                       # one row per billable service
   billing_code_type               # CPT, HCPCS, MS-DRG, etc.
   billing_code
   billing_code_type_version
   name, description
   negotiation_arrangement         # ffs, bundle, capitation
   negotiated_rates[]
      provider_groups[]
         npi[]                     # provider identifiers
         tin {type, value}         # tax id type (ein / npi)
      negotiated_prices[]
         negotiated_type           # negotiated, derived, fee schedule, percentage
         negotiated_rate           # the dollar amount
         expiration_date
         service_code[]            # place-of-service codes
         billing_class             # professional / institutional
         billing_code_modifier[]
      bundled_codes[]              # only if negotiation_arrangement = bundle
```

The grain that matters for analysis is **one row per (plan, billing_code, provider_group, negotiated_price)** — that's what your fact table looks like.

A practical note: not every file has every field. Some providers omit `billing_code_modifier`, `service_code`, `expiration_date`. Build your parser to tolerate missing keys, not assert on them.

---

## 1. Environment setup

### 1.1 Local machine

You'll need:

- Python 3.11 (3.12 also fine; avoid 3.13 for now, some libs lag)
- ~150 GB free disk (you will not keep all 1000 raw files — see §3.3)
- Stable internet, ideally wired

### 1.2 Snowflake trial

1. Go to signup.snowflake.com → sign up with a personal email (the trial gives $400 of credits, 30 days).
2. Pick **AWS** as cloud, **US East (N. Virginia)** as region — matches where UHC's files are served from, so the COPY INTO from external stage will be faster if you go that route.
3. Pick **Enterprise** edition for the trial (Standard works too; Enterprise gives you materialized views which you might use).
4. After signup, log in via the web UI (Snowsight) and verify you have the `ACCOUNTADMIN` role.
5. Note your **account identifier** — something like `abc12345.us-east-1` — you'll need it for the Python connector.

### 1.3 Python environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install requests ijson orjson pandas pyarrow snowflake-connector-python[pandas] tqdm python-dotenv
# pyspark only if you genuinely intend to use it - see §4.1
pip install pyspark==3.5.1
```

Pin versions in a `requirements.txt` so the interviewer can reproduce it. Don't pip freeze the whole venv — only the libs you actually import. A clean 8-line requirements file looks more deliberate than a 60-line one.

### 1.4 Project layout

```
uhc-tic/
  data/
    index/                       # the table-of-contents JSON
    raw/                         # downloaded *.json.gz (delete after parse)
    parquet/                     # flattened parquet, partitioned by plan
  notebooks/
    01_explore_index.ipynb
    02_sample_inspect.ipynb
    03_insights.ipynb
  src/
    download.py
    parse.py
    load_snowflake.py
  sql/
    01_ddl.sql
    02_load.sql
    03_analysis.sql
  .env                           # SNOWFLAKE_USER, _PASSWORD, _ACCOUNT, etc.
  requirements.txt
  README.md
```

This layout looks like something you'd actually build, not generated. Keep `.env` out of git with a `.gitignore`.

---

## 2. Get the index file

UHC publishes the index URL on the landing page. As of the last several months it follows the pattern:

```
https://transparency-in-coverage.uhc.com/api/v1/uhc/<YYYY-MM-DD>_<reporting_entity>_index.json.gz
```

The site exposes a "Download Index File" link — copy that URL into a variable, do not hard-code it. Sometimes there are multiple index files (one per legal entity); pick the largest, which is usually the United HealthCare Insurance Company file.

```python
# src/download.py — index portion only
import requests, gzip, json, pathlib

INDEX_URL = "<paste from the UHC site>"

def fetch_index(url, out_dir="data/index"):
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    fname = url.rsplit("/", 1)[-1]
    out = pathlib.Path(out_dir) / fname
    if out.exists():
        return out
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)
    return out
```

The index itself is gzipped. It's a few hundred MB compressed, ~5 GB uncompressed. Do **not** load it with `json.load` — stream it with `ijson`.

```python
import ijson, gzip

def iter_in_network_locations(index_path, limit=1000):
    """Yield (plan_name, plan_id, location_url) tuples from the index."""
    seen = 0
    with gzip.open(index_path, "rb") as f:
        # the path "reporting_structure.item" iterates one block at a time
        for rs in ijson.items(f, "reporting_structure.item"):
            plans = rs.get("reporting_plans", [])
            for inf in rs.get("in_network_files", []):
                # pair each in-network file URL with the first plan that owns it
                p = plans[0] if plans else {}
                yield (p.get("plan_name"), p.get("plan_id"), inf.get("location"))
                seen += 1
                if seen >= limit:
                    return
```

> Walkthrough note: when the interviewer asks "why ijson", the answer is *the index is 5 GB uncompressed and Python's stdlib `json` would OOM the laptop*. Practice saying that out loud.

Save the 1000 URLs to a CSV (`data/index/manifest.csv`) before downloading anything. That manifest is your worklist — if a download fails, you re-run against the manifest.

---

## 3. Download the first 1000 files

### 3.1 The disk-space problem

This is the real reason this task is non-trivial. Median UHC in-network file is ~300 MB compressed, ~3–8 GB uncompressed. The first 1000 entries in the index include some very large files (some plans have 30+ GB single files).

**Practical decision**: download all 1000 *headers* first (HTTP HEAD requests) to get `Content-Length`. Then download in size order, smallest first. You can park the giant ones to last and decide whether you actually need them.

```python
def get_sizes(manifest):
    sizes = []
    for plan_name, plan_id, url in manifest:
        try:
            h = requests.head(url, allow_redirects=True, timeout=20)
            sizes.append((plan_name, plan_id, url, int(h.headers.get("Content-Length", 0))))
        except Exception as e:
            sizes.append((plan_name, plan_id, url, -1))
    return sizes
```

Sort ascending by size, persist to `manifest_sized.csv`. Now you have a sensible download order.

### 3.2 Downloader

```python
import concurrent.futures as cf
from pathlib import Path
from tqdm import tqdm

def download_one(url, out_dir="data/raw"):
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    name = url.rsplit("/", 1)[-1].split("?")[0]
    out = Path(out_dir) / name
    if out.exists() and out.stat().st_size > 0:
        return out, "skipped"
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        tmp = out.with_suffix(out.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(8 * 1024 * 1024):  # 8 MB
                f.write(chunk)
        tmp.rename(out)
    return out, "ok"

def download_many(rows, workers=4):
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(download_one, r[2]): r for r in rows}
        for fu in tqdm(cf.as_completed(futs), total=len(futs)):
            yield fu.result()
```

Keep `workers` low (4–6). UHC's CDN throttles aggressive parallelism and you'll start getting 429s. Log every URL + status to a CSV so you can pick up where you left off.

### 3.3 The "you can't keep all 1000 files" reality

Do **stream-parse → write parquet → delete the raw file** in a loop. The intermediate parquet of just the columns you care about is roughly 1/30th the size of the raw gz. So 1000 files at ~300 MB raw compressed = 300 GB; the parquet output is 8–12 GB total. That fits.

Put this in your walkthrough: "I considered keeping the raw files, decided against it, and here's the math." Interviewers love that.

---

## 4. Parsing — the part everyone underestimates

### 4.1 PySpark vs. Python streaming

The task says "Python / PySpark". Most candidates default to PySpark because the word "big data" is in their head. PySpark's JSON reader struggles with the deeply nested `in_network[].negotiated_rates[].negotiated_prices[]` triple-explode and you end up writing UDFs anyway.

What actually works better:

- **`ijson`** for streaming each file (constant memory regardless of file size).
- **Flatten in Python** to a list of dicts at the (code, provider_group, price) grain.
- **`pyarrow`** to write parquet in batches.
- **Save PySpark for the analysis step in Snowflake/Snowpark** — or skip it. Snowflake will outperform local Spark on the joins you actually care about.

If you choose to use PySpark in the walkthrough anyway (some interviewers expect it on the resume), use it for the final aggregation pass over the parquet output. That gives you something to talk about without making your life hard during ingest.

### 4.2 The flattener

```python
# src/parse.py
import gzip, ijson, pyarrow as pa, pyarrow.parquet as pq
from pathlib import Path

WANTED_CODE_TYPES = {"CPT", "HCPCS", "MS-DRG"}  # drop the noisy ones

def flatten(file_path):
    """Yield one dict per (code, provider_group, negotiated_price)."""
    with gzip.open(file_path, "rb") as f:
        # top-level scalars
        scalars = {}
        parser = ijson.parse(f)
        for prefix, evt, val in parser:
            if prefix in ("reporting_entity_name", "plan_name", "plan_id",
                          "plan_market_type", "last_updated_on", "version"):
                scalars[prefix] = val
            if prefix == "in_network" and evt == "start_array":
                break

        # now stream in_network items
        for inn in ijson.items(parser, "in_network.item", multiple_values=False):
            if inn.get("billing_code_type") not in WANTED_CODE_TYPES:
                continue
            for nr in inn.get("negotiated_rates", []):
                # provider groups can have many NPIs; we'll explode at load time in SQL
                for pg in nr.get("provider_groups", []):
                    npis = pg.get("npi") or []
                    tin = pg.get("tin") or {}
                    for price in nr.get("negotiated_prices", []):
                        yield {
                            "plan_name": scalars.get("plan_name"),
                            "plan_id": scalars.get("plan_id"),
                            "plan_market_type": scalars.get("plan_market_type"),
                            "reporting_entity_name": scalars.get("reporting_entity_name"),
                            "last_updated_on": scalars.get("last_updated_on"),
                            "billing_code_type": inn.get("billing_code_type"),
                            "billing_code": inn.get("billing_code"),
                            "service_name": inn.get("name"),
                            "service_description": inn.get("description"),
                            "negotiation_arrangement": inn.get("negotiation_arrangement"),
                            "tin_type": tin.get("type"),
                            "tin_value": tin.get("value"),
                            "npi_count": len(npis),
                            "npi_sample": npis[0] if npis else None,
                            "negotiated_type": price.get("negotiated_type"),
                            "negotiated_rate": price.get("negotiated_rate"),
                            "billing_class": price.get("billing_class"),
                            "expiration_date": price.get("expiration_date"),
                            "service_codes": ",".join(price.get("service_code") or []),
                            "modifiers": ",".join(price.get("billing_code_modifier") or []),
                        }

def file_to_parquet(in_path, out_dir="data/parquet", batch=200_000):
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / (Path(in_path).stem + ".parquet")
    if out_path.exists():
        return out_path
    writer = None
    buf = []
    for row in flatten(in_path):
        buf.append(row)
        if len(buf) >= batch:
            tbl = pa.Table.from_pylist(buf)
            if writer is None:
                writer = pq.ParquetWriter(out_path, tbl.schema, compression="zstd")
            writer.write_table(tbl)
            buf = []
    if buf:
        tbl = pa.Table.from_pylist(buf)
        if writer is None:
            writer = pq.ParquetWriter(out_path, tbl.schema, compression="zstd")
        writer.write_table(tbl)
    if writer:
        writer.close()
    return out_path
```

Run this in a loop: parse a file, write parquet, **delete the raw gz**, move on. With 4 worker processes you'll do the 1000 files in roughly 6–12 hours depending on your bandwidth. Start it overnight.

### 4.3 Sanity-check before you load

Pick three parquet files at random and inspect them in a notebook:

```python
import pandas as pd
df = pd.read_parquet("data/parquet/<some-file>.parquet")
df.shape, df["billing_code_type"].value_counts(), df["negotiated_rate"].describe()
```

Things to look for and ask yourself out loud (you'll repeat these in the walkthrough):

- Are there negative rates? Zero rates? Multi-million-dollar rates? They exist and they're real — drug HCPCS codes can carry huge negotiated values per unit.
- What fraction of rows are professional vs institutional?
- How many distinct billing codes per plan? (Usually tens of thousands.)

Write down two or three of these observations as you go. Those become "insights" later — and they're real ones, not generic ones.

---

## 5. Load into Snowflake

### 5.1 Schema

```sql
-- sql/01_ddl.sql
CREATE OR REPLACE DATABASE uhc_tic;
CREATE OR REPLACE SCHEMA uhc_tic.staging;
CREATE OR REPLACE SCHEMA uhc_tic.analytics;

CREATE OR REPLACE TABLE uhc_tic.staging.rates_raw (
  plan_name STRING,
  plan_id STRING,
  plan_market_type STRING,
  reporting_entity_name STRING,
  last_updated_on DATE,
  billing_code_type STRING,
  billing_code STRING,
  service_name STRING,
  service_description STRING,
  negotiation_arrangement STRING,
  tin_type STRING,
  tin_value STRING,
  npi_count NUMBER,
  npi_sample STRING,
  negotiated_type STRING,
  negotiated_rate NUMBER(20,4),
  billing_class STRING,
  expiration_date DATE,
  service_codes STRING,
  modifiers STRING,
  source_file STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE FILE FORMAT uhc_tic.staging.pq_fmt
  TYPE = PARQUET;

CREATE OR REPLACE STAGE uhc_tic.staging.local_pq
  FILE_FORMAT = uhc_tic.staging.pq_fmt;
```

### 5.2 Upload

Use SnowSQL or the Python connector. Python is cleaner:

```python
# src/load_snowflake.py
import os, glob
import snowflake.connector
from dotenv import load_dotenv
load_dotenv()

conn = snowflake.connector.connect(
    user=os.environ["SF_USER"],
    password=os.environ["SF_PASSWORD"],
    account=os.environ["SF_ACCOUNT"],
    warehouse="COMPUTE_WH",
    database="UHC_TIC",
    schema="STAGING",
    role="SYSADMIN",
)
cur = conn.cursor()

files = sorted(glob.glob("data/parquet/*.parquet"))
for fp in files:
    cur.execute(f"PUT file://{os.path.abspath(fp)} @local_pq AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

cur.execute("""
COPY INTO rates_raw (
  plan_name, plan_id, plan_market_type, reporting_entity_name, last_updated_on,
  billing_code_type, billing_code, service_name, service_description,
  negotiation_arrangement, tin_type, tin_value, npi_count, npi_sample,
  negotiated_type, negotiated_rate, billing_class, expiration_date,
  service_codes, modifiers, source_file
)
FROM (
  SELECT
    $1:plan_name::string,
    $1:plan_id::string,
    $1:plan_market_type::string,
    $1:reporting_entity_name::string,
    TRY_TO_DATE($1:last_updated_on::string),
    $1:billing_code_type::string,
    $1:billing_code::string,
    $1:service_name::string,
    $1:service_description::string,
    $1:negotiation_arrangement::string,
    $1:tin_type::string,
    $1:tin_value::string,
    $1:npi_count::number,
    $1:npi_sample::string,
    $1:negotiated_type::string,
    $1:negotiated_rate::number(20,4),
    $1:billing_class::string,
    TRY_TO_DATE($1:expiration_date::string),
    $1:service_codes::string,
    $1:modifiers::string,
    METADATA$FILENAME
  FROM @local_pq
)
FILE_FORMAT = (FORMAT_NAME = pq_fmt)
ON_ERROR = 'CONTINUE';
""")
```

Notes for the walkthrough:

- The trial warehouse is XS by default. **Resize to MEDIUM** while you load, then drop it back to XS or suspend. The few extra credits are worth it on 50–100 million rows.
- `ON_ERROR = 'CONTINUE'` is intentional — you'd rather know what broke than fail the whole load. After COPY, run `SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))` to see error counts per file.
- `METADATA$FILENAME` is the lineage trick — every row knows which parquet it came from.

### 5.3 Build the analytics layer

```sql
-- sql/02_load.sql
CREATE OR REPLACE TABLE uhc_tic.analytics.rates AS
SELECT *
FROM uhc_tic.staging.rates_raw
WHERE negotiated_rate IS NOT NULL
  AND negotiated_rate > 0
  AND billing_code IS NOT NULL;

CREATE OR REPLACE TABLE uhc_tic.analytics.codes AS
SELECT DISTINCT
  billing_code_type, billing_code, service_name, service_description
FROM uhc_tic.analytics.rates;
```

You now have a clean fact table you can query.

---

## 6. The insights — this is what gets you hired

The interviewer doesn't care that you loaded the data. They care what you noticed.

Pick **three or four** insights, not ten. Each one should have: a question, a query, a number, and a "so what".

### 6.1 Suggested angles (pick the ones that surprise you when you run them)

**Price variation for the same procedure.** Across providers, the negotiated price for the same CPT code varies by orders of magnitude. MRI of the brain (CPT 70551) commonly ranges from $200 to $4,000+ within the same plan network. Show the histogram, name the 10x/90x percentile gap, name a few specific codes.

```sql
WITH common AS (
  SELECT billing_code, COUNT(*) c
  FROM analytics.rates
  WHERE billing_code_type = 'CPT'
  GROUP BY 1
  ORDER BY c DESC
  LIMIT 50
)
SELECT r.billing_code, r.service_name,
       MIN(r.negotiated_rate)  AS p_min,
       APPROX_PERCENTILE(r.negotiated_rate, 0.5)  AS p_median,
       APPROX_PERCENTILE(r.negotiated_rate, 0.95) AS p95,
       MAX(r.negotiated_rate)  AS p_max,
       MAX(r.negotiated_rate) / NULLIF(MIN(r.negotiated_rate), 0) AS ratio_max_min
FROM analytics.rates r
JOIN common c USING (billing_code)
GROUP BY 1, 2
ORDER BY ratio_max_min DESC;
```

**Negotiation arrangement mix.** What fraction of rates are FFS vs bundle vs capitation? Bundles are rare in the data and that itself is interesting — it tells you something about how the contracts are structured.

**Plan-level concentration.** How many distinct billing codes per plan? How many providers (NPIs) per plan? You will find that some plans have 50,000 codes priced and others have 5,000. Worth asking why.

**Outliers.** A small fraction of rows have negotiated_rate values that are clearly junk (1 cent, $1, or eight-figure values). Quantify them. Don't drop them silently — explain what you'd want to confirm with the data steward before excluding.

### 6.2 Add-on data ("add on data that would help explaining the insights further" — from the brief)

Two cheap joins that make your analysis 10x more credible:

- **CMS NPPES NPI Registry** — a free public dataset of every US healthcare provider keyed by NPI. Lets you turn NPIs into provider names, specialties, and ZIP codes. Download the monthly dissemination file (https://download.cms.gov/nppes/NPI_Files.html), filter to the NPIs in your data, load to Snowflake as `analytics.providers`. Suddenly you can say "MRI prices in the 90th percentile are concentrated in Manhattan ZIPs", which is a real, defensible insight.
- **CMS HCPCS / CPT descriptors** — your `service_description` field is sometimes missing or terse. The official CMS HCPCS quarterly file fills the gaps.

Mention both even if you only do the NPI one. Interviewers reward "I knew about the second one and chose not to do it because…".

### 6.3 The visualization

Use a Snowflake-native option for the walkthrough rather than building a separate dashboard:

- **Snowsight charts** — built into the Snowflake UI. Run the query, click "Chart". Good enough for an interview screen-share, and it keeps everything in one tool.
- Or **a single Jupyter notebook** with `snowflake-connector-python` → `pandas` → `matplotlib`. Three or four charts, one per insight. No Tableau, no Streamlit, no fluff.

---

## 7. The walkthrough document

This is the artifact you actually send back. Keep it as a README in the repo (not a polished Word doc) — that signals "engineer", not "consultant".

Structure it as:

1. **What I built** — three sentences. End-to-end pipeline, 1000 files, X million rows in Snowflake.
2. **How to run it** — `make download`, `make parse`, `make load`, or whatever scripts you wrote. Copy-pasteable.
3. **Decisions and trade-offs** — three or four bullets. *Streamed with ijson instead of loading into RAM. Deleted raw files after parsing. Used XS warehouse for loads, MEDIUM for ad-hoc queries. Skipped PySpark for ingest because…*
4. **Insights** — the three or four you picked, with the chart and the number.
5. **What I'd do next** — incremental loads, dbt models, a Snowflake task to refresh weekly, NPPES join. Don't actually do these. Just show you know they exist.

---

## 8. Things that make assessments look AI-generated (and how to avoid them)

This is the bit you asked about specifically. The code is rarely the tell — the tell is the *commentary*. Mistakes that scream "LLM wrote this":

- Every function has a perfectly-formed docstring with Args/Returns/Raises sections. Real engineers write a one-line comment or nothing. Mix it up.
- README headings like "Overview", "Conclusion", "Future Work". Real engineers use messy, specific headings like "Why I didn't use Spark" or "Notes to self".
- Round numbers in "insights". "We found pricing varies by 10x" is suspicious. "MRI brain without contrast (CPT 70551) ranges $187 to $4,012 across 412 provider groups" is real.
- Emoji in markdown. Almost no engineer puts emoji in a technical README. Don't.
- Bulleted lists where prose would do. AI defaults to bullets. Write at least two paragraphs of plain prose in your README.
- "Best practices" language ("following best practices, I…"). Just say what you did.
- Perfect commit history with conventional commits and clean messages. Mix in a couple of "wip", "fix typo", "actually fix the date parsing" commits — that's how real work looks.
- No errors, no edge cases mentioned. Real ingest has surprises. Mention two or three things that broke and how you handled them.
- Generic insight templates ("This data could be useful for…"). Replace with specific, opinionated observations.
- Comments that restate the code ("# loop over the rows"). Comments should say *why*, not *what*.

The cheapest tell to fix: write your README in your own voice in a single sitting, in plain language, in Markdown, with the headings you'd naturally use. If you find yourself reaching for "Furthermore" or "Additionally", you're drifting into LLM-tone. Use "also" and "and then".

Two more practical moves:

- After you build everything, do a hand-edit pass on the README and notebooks. Add one or two genuinely personal observations ("the 10 GB Anthem-look-alike plan file was an outlier and I excluded it — note here for next time"). Those land as authentic.
- Practice the walkthrough out loud once. The interviewer will ask "why parquet?" "why ijson?" "why MEDIUM warehouse?". If you can answer in your own words, the provenance question never comes up.

---

## 9. Time budget (4–7 days, per the brief)

- **Day 1:** read the spec, set up Snowflake trial, set up Python env, download and inspect the index file.
- **Day 2:** build the downloader and the parquet flattener; run them on ~10 files end-to-end to validate the schema.
- **Day 3:** kick off the full 1000-file download/parse in the background. Build the Snowflake DDL and a small COPY INTO on the 10 sample files. Verify counts.
- **Day 4:** full load. Sanity-check row counts, nulls, duplicates. Build the analytics tables.
- **Day 5:** write the three or four queries for your insights. Iterate until they tell a story.
- **Day 6:** README, charts, walkthrough rehearsal. Hand-edit pass.
- **Day 7:** buffer for the thing that broke on day 4 that you haven't found yet.

---

## 10. Final checklist before you send it back

- Repo is a single git repo, public or zipped, with a README at the root.
- `requirements.txt` is short and pinned.
- `.env.example` exists, `.env` does not.
- The three or four insights have a query, a chart, and one paragraph of interpretation each.
- You can answer "why this design" for every meaningful decision.
- You ran the whole thing end-to-end one final time on a clean checkout. (Most "this doesn't reproduce" rejections come from skipping this.)
