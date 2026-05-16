# QUICKSTART - UHC Data Pipeline

## ONE COMMAND TO RUN EVERYTHING

```powershell
python src/pipeline.py
```

That's it. The pipeline will:
1. Read all index files from `files/index_file/`
2. Extract all download URLs
3. Download files one by one
4. Parse each to parquet
5. Upload to S3
6. Load to Snowflake
7. Track progress in `files/index_file/progress.csv`

## Setup (One Time)

1. Activate environment:
```powershell
.\.venv\Scripts\Activate.ps1
```

2. Set STORAGE_MODE in `.env`:
```
STORAGE_MODE=LOCAL  # Default: saves parquet locally
# OR
STORAGE_MODE=S3     # Uploads parquet to AWS S3
```

3. If using S3, ensure `.env` has:
```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET=...
```

## How It Works

### Progress Tracking
- Pipeline saves progress to `files/index_file/progress.csv`
- Each row: [url, plan_name, plan_id, status, ...]
- Status: pending | parsed | failed

### Resume from Stop
If you stop the pipeline (Ctrl+C) and run it again:
- It reads progress.csv
- Skips already processed files
- Continues from the next pending file
- No duplicate work

### Example Flow
```
Run 1: Processes files 1-5, then stopped
  files/index_file/progress.csv shows: file1=parsed, file2=parsed, ... file6=pending

Run 2: python src/pipeline.py
  Skips 1-5 (already parsed)
  Continues from file 6
```

## Output Files

After running:
- `files/network_files/*.json` - Downloaded but not yet parsed are deleted after success
- `data/parquet/*.parquet` (LOCAL mode) - Parquet files locally
- `s3://bucket/parquet/` (S3 mode) - Parquet files in S3
- `files/index_file/progress.csv` - Progress tracking
- Snowflake table `uhc_tic.staging.rates_raw` - Loaded data

## Monitoring

### During Run
- Watch console output for status
- File count and progress shown

### After Run
Check `files/index_file/progress.csv`:
```csv
url,plan_name,plan_id,status,error_message
https://...,Select-Plus,36-2739571,parsed,
https://...,Choice,36-2739571,failed,Download failed: timeout
```

### In Snowflake
```sql
SELECT COUNT(*) FROM uhc_tic.staging.rates_raw;
SELECT status, COUNT(*) FROM uhc_tic.staging.rates_raw GROUP BY status;
```

## Troubleshooting

### "No index files found"
- Check `files/index_file/` directory
- Need at least one `*index.json` file

### "Download failed: timeout"
- Network issue
- Run again - pipeline will resume and retry

### "Parse failed"
- JSON parsing error
- Check console for error message
- Mark as failed in progress.csv, can retry later

### "Load failed"
- Snowflake connection issue
- Check `.env` credentials
- Check warehouse is running

## Files Changed

- `src/pipeline.py` - NEW - Master orchestrator
- `src/config.py` - Config and AWS setup
- `src/download.py` - Download logic (called by pipeline)
- `src/parse.py` - Parsing logic (called by pipeline)
- `src/load_snowflake.py` - Loading logic (called by pipeline)

## Questions?

All the heavy lifting happens in `src/pipeline.py`. Read it for details on each step.
