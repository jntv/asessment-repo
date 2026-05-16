import json
import gzip
import ijson
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from tqdm import tqdm
from config import upload_to_s3, S3_PREFIX, STORAGE_MODE, validate_aws_config

WANTED_CODE_TYPES = {"CPT", "HCPCS", "MS-DRG", "RC"}  # RC = Revenue Code

if STORAGE_MODE == "S3":
    try:
        validate_aws_config()
        print("AWS S3 configured")
    except ValueError as e:
        print(f"Warning: {e}")
else:
    print("Storage mode: LOCAL")


def flatten(file_path):
    """Stream-parse JSON file and yield flattened rows."""
    file_path_str = str(file_path)
    f = gzip.open(file_path, "rb") if file_path_str.endswith(".gz") else open(file_path, "r")

    try:
        if file_path_str.endswith(".gz"):
            parser = ijson.parse(f)
            scalars = {}
            for prefix, evt, val in parser:
                if prefix in ("reporting_entity_name", "plan_name", "plan_id", "plan_market_type", "last_updated_on"):
                    scalars[prefix] = val
                if prefix == "in_network" and evt == "start_array":
                    break

            for inn in ijson.items(parser, "in_network.item", multiple_values=False):
                if inn.get("billing_code_type") not in WANTED_CODE_TYPES:
                    continue
                for nr in inn.get("negotiated_rates", []):
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
        else:
            with open(file_path, "r") as json_f:
                data = json.load(json_f)
                scalars = {
                    "reporting_entity_name": data.get("reporting_entity_name"),
                    "plan_name": data.get("plan_name"),
                    "plan_id": data.get("plan_id"),
                    "plan_market_type": data.get("plan_market_type"),
                    "last_updated_on": data.get("last_updated_on"),
                }
                for inn in data.get("in_network", []):
                    if inn.get("billing_code_type") not in WANTED_CODE_TYPES:
                        continue
                    for nr in inn.get("negotiated_rates", []):
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
    finally:
        f.close()


def file_to_parquet(in_path, out_dir="data/parquet", batch=200000):
    """Convert JSON to parquet, streaming to control memory."""
    out_dir = Path(out_dir)
    out_path = out_dir / (Path(in_path).stem + ".parquet")
    out_dir.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        return out_path, "skipped"

    writer = None
    buf = []
    row_count = 0

    try:
        for row in flatten(in_path):
            buf.append(row)
            row_count += 1
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
    except Exception as e:
        if out_path.exists():
            out_path.unlink()
        return None, f"error: {e}"

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
