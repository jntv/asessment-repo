import json
import gzip
import ijson
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from tqdm import tqdm
import logging
import psutil
from config import upload_to_s3, S3_PREFIX, STORAGE_MODE, validate_aws_config

logger = logging.getLogger(__name__)

def get_memory_mb():
    """Get current memory usage in MB"""
    return psutil.Process().memory_info().rss / 1024 / 1024

# Parse ALL code types - don't filter, let Snowflake handle filtering if needed
# Removed: WANTED_CODE_TYPES = {"CPT", "HCPCS", "MS-DRG", "RC"}

if STORAGE_MODE == "S3":
    try:
        validate_aws_config()
        print("AWS S3 configured")
    except ValueError as e:
        print(f"Warning: {e}")
else:
    print("Storage mode: LOCAL")


def flatten(file_path):
    """Stream-parse JSON file using ijson (memory-efficient)."""
    file_path_str = str(file_path)

    # Try to open as gzip first if it has .gz extension, fall back to plain JSON
    f = None
    if file_path_str.endswith(".gz"):
        try:
            f = gzip.open(file_path, "rb")
            # Test that it's actually gzipped by reading a bit
            f.read(1)
            f.seek(0)
        except (OSError, gzip.BadGzipFile):
            # Not actually gzipped, try plain JSON
            logger.warning(f"File {Path(file_path).name} has .gz extension but is not gzipped, treating as plain JSON")
            if f:
                f.close()
            f = open(file_path, "rb")
    else:
        f = open(file_path, "rb")

    try:
        # Use ijson for both .gz and .json files (streaming, not loading into memory)
        parser = ijson.parse(f)
        scalars = {}

        # Extract scalar values from top level
        for prefix, evt, val in parser:
            if prefix in ("reporting_entity_name", "plan_name", "plan_id", "plan_market_type", "last_updated_on"):
                scalars[prefix] = val
            if prefix == "in_network" and evt == "start_array":
                break

        # Stream through in_network items
        for inn in ijson.items(parser, "in_network.item", multiple_values=False):
            # Process ALL code types (CPT, HCPCS, MS-DRG, RC, CDT, NDC, etc.)
            # No filtering - get complete dataset
            for nr in inn.get("negotiated_rates", []):
                for pg in nr.get("provider_groups", []):
                    npis = pg.get("npi") or []
                    tin = pg.get("tin") or {}
                    for price in nr.get("negotiated_prices", []):
                        # Safely build service_codes and modifiers, defaulting to empty string if problematic
                        try:
                            service_codes = ",".join(str(x) for x in (price.get("service_code") or []) if x is not None) or None
                        except (TypeError, ValueError):
                            service_codes = None

                        try:
                            modifiers = ",".join(str(x) for x in (price.get("billing_code_modifier") or []) if x is not None) or None
                        except (TypeError, ValueError):
                            modifiers = None

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
                            "service_codes": service_codes,
                            "modifiers": modifiers,
                        }
    finally:
        f.close()


def file_to_parquet(in_path, out_dir="data/parquet", batch=1000):
    """Convert JSON to parquet, streaming to control memory."""
    out_dir = Path(out_dir)
    out_path = out_dir / (Path(in_path).stem + ".parquet")
    out_dir.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        return out_path, "skipped"

    writer = None
    buf = []
    row_count = 0
    max_memory = 0
    memory_start = get_memory_mb()

    logger.info(f"Starting parse of {Path(in_path).name}. Initial memory: {memory_start:.1f}MB")

    try:
        for row in flatten(in_path):
            buf.append(row)
            row_count += 1

            # Log memory every 10k rows
            if row_count % 10000 == 0:
                current_mem = get_memory_mb()
                max_memory = max(max_memory, current_mem)
                logger.info(f"  Parsed {row_count} rows. Memory: {current_mem:.1f}MB (peak: {max_memory:.1f}MB)")

            if len(buf) >= batch:
                logger.debug(f"  Writing batch of {len(buf)} rows at row {row_count}. Memory before write: {get_memory_mb():.1f}MB")
                tbl = pa.Table.from_pylist(buf)
                if writer is None:
                    writer = pq.ParquetWriter(out_path, tbl.schema, compression="zstd")
                writer.write_table(tbl)
                mem_after = get_memory_mb()
                max_memory = max(max_memory, mem_after)
                logger.debug(f"  Batch written. Memory after: {mem_after:.1f}MB")
                buf = []

        if buf:
            logger.info(f"Writing final batch of {len(buf)} rows. Memory: {get_memory_mb():.1f}MB")
            tbl = pa.Table.from_pylist(buf)
            if writer is None:
                writer = pq.ParquetWriter(out_path, tbl.schema, compression="zstd")
            writer.write_table(tbl)
            mem_after = get_memory_mb()
            max_memory = max(max_memory, mem_after)
            logger.info(f"Final batch written. Memory: {mem_after:.1f}MB")

        if writer:
            writer.close()

        logger.info(f"Parse complete. Total rows: {row_count}. Peak memory: {max_memory:.1f}MB (delta: {max_memory - memory_start:.1f}MB)")
    except Exception as e:
        current_mem = get_memory_mb()
        logger.exception(f"Error during parsing at row {row_count}. Current memory: {current_mem:.1f}MB. Error: {e}")
        if out_path.exists():
            out_path.unlink()
        return None, f"error: {e}"

    # Check if any rows were actually parsed
    if row_count == 0:
        logger.warning(f"No rows parsed from {Path(in_path).name} - file may be empty or has no in_network items")
        return None, "error: 0 rows parsed, no file created"

    if STORAGE_MODE == "S3":
        try:
            s3_key = f"{S3_PREFIX}{out_path.name}"
            s3_uri = upload_to_s3(str(out_path), s3_key)
            out_path.unlink()
            return s3_uri, f"ok ({row_count} rows, uploaded to S3)"
        except Exception as s3_error:
            return None, f"error uploading to S3: {s3_error}"
    else:
        return str(out_path), f"ok ({row_count} rows, saved to {out_path})"


def parse_all(raw_dir="files/network_files", out_dir="data/parquet"):
    """Parse all JSON files in directory."""
    raw_dir = Path(raw_dir)
    raw_files = sorted(raw_dir.glob("*.json"))
    print(f"Found {len(raw_files)} files")

    log_data = []
    for json_file in tqdm(raw_files):
        out_path, status = file_to_parquet(json_file, out_dir)
        log_data.append((json_file.name, status))
        if "ok" in status and json_file.exists():
            json_file.unlink()

    import csv
    log_path = Path("files/index_file/parse_log.csv")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w", newline="") as f:
        csv.writer(f).writerows([["filename", "status"]] + log_data)
    print("Parse complete!")


if __name__ == "__main__":
    print("Use pipeline.py instead: python src/pipeline.py")
