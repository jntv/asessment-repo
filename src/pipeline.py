"""
UHC Healthcare Data Pipeline
Handles downloading, parsing, and loading healthcare pricing data from UnitedHealthcare
"""

import os, sys, json, csv, logging, psutil
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

from config import STORAGE_MODE, validate_aws_config
from download import download_one
from parse import file_to_parquet
from load_snowflake import load_snowflake

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

INDEX_DIR = Path("files/index_file")
PROGRESS_FILE = INDEX_DIR / "progress.csv"
PROGRESS_COLUMNS = ["url", "plan_name", "plan_id", "reporting_entity", "status", "downloaded_at", "parsed_at", "error_message"]

def extract_urls_from_index():
    urls_list = []
    index_files = list(INDEX_DIR.glob("*index.json"))
    if not index_files:
        print("WARNING: No index files found")
        return []
    print(f"Found {len(index_files)} index file(s)")
    for index_file in index_files:
        print(f"  Reading {index_file.name}...")
        with open(index_file, 'r') as f:
            index_data = json.load(f)
        reporting_entity = index_data.get("reporting_entity_name", "Unknown")
        for structure in index_data.get("reporting_structure", []):
            reporting_plans = structure.get("reporting_plans", [])
            plan = reporting_plans[0] if reporting_plans else {}
            plan_name = plan.get("plan_name", "Unknown")
            plan_id = plan.get("plan_id", "Unknown")
            for file_ref in structure.get("in_network_files", []):
                url = file_ref.get("location")
                if url:
                    urls_list.append({
                        "url": url, "plan_name": plan_name, "plan_id": plan_id,
                        "reporting_entity": reporting_entity, "status": "pending",
                        "downloaded_at": "", "parsed_at": "", "error_message": ""
                    })
    print(f"Extracted {len(urls_list)} URLs")
    return urls_list

def load_progress():
    progress = {}
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r', newline='') as f:
            for row in csv.DictReader(f):
                progress[row['url']] = row
    return progress

def save_progress(all_urls):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=PROGRESS_COLUMNS)
        w.writeheader()
        w.writerows(all_urls)

def get_memory_usage():
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024

def process_file(url, plan_name, plan_id, reporting_entity):
    json_path = None
    try:
        print("  Downloading...")
        json_path, status = download_one(url)
        if not json_path:
            logger.warning(f"Download failed: {status}")
            return False, None, f"Download failed: {status}"

        json_size_mb = Path(json_path).stat().st_size / 1024 / 1024
        logger.info(f"Downloaded: {Path(json_path).name} ({json_size_mb:.1f}MB)")
        print(f"  Downloaded: {Path(json_path).name} ({json_size_mb:.1f}MB)")

        print("  Parsing to parquet...")
        parquet_path, parse_status = file_to_parquet(json_path)

        if not parquet_path:
            logger.error(f"Parse failed: {parse_status}")
            return False, None, f"Parse failed: {parse_status}"

        # Handle both local and S3 paths
        if isinstance(parquet_path, str) and parquet_path.startswith("s3://"):
            # S3 URI returned after upload - can't stat remotely
            filename = parquet_path.split('/')[-1]
            logger.info(f"Uploaded to S3: {parquet_path}")
            print(f"  Uploaded: {filename}")
        else:
            # Local path - get file size
            parquet_path = Path(parquet_path)
            parquet_size_mb = parquet_path.stat().st_size / 1024 / 1024
            logger.info(f"Parsed: {parquet_path.name} ({parquet_size_mb:.1f}MB)")
            print(f"  Parsed: {parquet_path.name} ({parquet_size_mb:.1f}MB)")

        # Clean up: Delete JSON file after successful parsing
        try:
            Path(json_path).unlink()
            logger.info(f"Cleaned up: {Path(json_path).name}")
        except Exception as cleanup_error:
            logger.warning(f"Could not delete {Path(json_path).name}: {cleanup_error}")

        return True, parquet_path, None
    except Exception as e:
        logger.exception(f"Error processing file: {e}")

        # Clean up on error too
        if json_path and Path(json_path).exists():
            try:
                Path(json_path).unlink()
            except:
                pass
        return False, None, str(e)

def main():
    print("\n" + "="*70)
    print("UHC Transparency-in-Coverage Data Pipeline")
    print("="*70)
    if STORAGE_MODE == "S3":
        try:
            validate_aws_config()
            print("Storage mode: S3")
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)
    else:
        print("Storage mode: LOCAL")
    print("\nStep 1: Extracting URLs from index files...")
    all_urls = extract_urls_from_index()
    if not all_urls:
        print("ERROR: No URLs found")
        sys.exit(1)
    print("\nStep 2: Loading progress...")
    progress = load_progress()
    for url_item in all_urls:
        if url_item['url'] in progress:
            url_item.update(progress[url_item['url']])
    pending = sum(1 for u in all_urls if u['status'] == 'pending')
    parsed = sum(1 for u in all_urls if u['status'] == 'parsed')
    failed = sum(1 for u in all_urls if u['status'] == 'failed')
    print(f"  Total: {len(all_urls)} | Pending: {pending} | Parsed: {parsed} | Failed: {failed}")
    
    if pending > 0:
        print(f"\nStep 3: Processing {pending} pending file(s)...\n")

        # Group all URLs by their value to deduplicate
        unique_urls = {}
        for url_item in all_urls:
            url = url_item['url']
            if url not in unique_urls:
                unique_urls[url] = []
            unique_urls[url].append(url_item)

        # Count unique URLs that still need processing
        pending_unique = sum(1 for url_items in unique_urls.values()
                            if any(item['status'] == 'pending' for item in url_items))
        print(f"  {pending} total rows, but only {pending_unique} unique URLs to download\n")

        count = 0
        for url, url_items in unique_urls.items():
            # Skip if all rows with this URL are already done
            pending_items = [item for item in url_items if item['status'] == 'pending']
            if not pending_items:
                continue

            count += 1
            # Use first item's metadata for processing
            url_item = pending_items[0]
            plan_name = url_item['plan_name']
            plan_id = url_item['plan_id']
            reporting_entity = url_item['reporting_entity']
            filename = urlparse(url).path.split('/')[-1].split('?')[0]

            plans_using_file = len(url_items)
            print(f"[{count}] {filename}")
            print(f"    Used by {plans_using_file} plan(s) | First: {plan_name} ({plan_id})")

            success, parquet_path, error_msg = process_file(url, plan_name, plan_id, reporting_entity)

            # Mark ALL rows with this URL with the same status
            timestamp = datetime.now().isoformat()
            for item in url_items:
                if success:
                    item['status'] = 'parsed'
                    item['parsed_at'] = timestamp
                else:
                    item['status'] = 'failed'
                    item['error_message'] = error_msg

            if success:
                print(f"  SUCCESS (applies to {plans_using_file} plans)\n")
            else:
                print(f"  FAILED: {error_msg}\n")

            save_progress(all_urls)
    
    parsed_count = sum(1 for u in all_urls if u['status'] == 'parsed')
    if parsed_count > 0:
        print(f"\nStep 4: Loading {parsed_count} parsed file(s) to Snowflake...\n")
        try:
            load_snowflake()
            print("Load complete!\n")
        except Exception as e:
            print(f"Load failed: {e}\n")
    
    print("="*70)
    print("SUMMARY")
    print("="*70)
    final_pending = sum(1 for u in all_urls if u['status'] == 'pending')
    final_parsed = sum(1 for u in all_urls if u['status'] == 'parsed')
    final_failed = sum(1 for u in all_urls if u['status'] == 'failed')
    print(f"Total files:    {len(all_urls)}")
    print(f"Pending:        {final_pending}")
    print(f"Parsed:         {final_parsed}")
    print(f"Failed:         {final_failed}")
    print(f"\nProgress CSV:   {PROGRESS_FILE}")
    print("="*70 + "\n")
    if final_pending > 0:
        print(f"{final_pending} file(s) remaining. Run again to continue.\n")
    else:
        print("All files processed!\n")

if __name__ == "__main__":
    main()
