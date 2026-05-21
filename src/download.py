import requests
import gzip
import json
import pathlib
import csv
import ijson
from tqdm import tqdm

# Index URL from https://transparency-in-coverage.uhc.com/
# Updated 2026-05-16: Using current mrfstore.uhc.com endpoint with SAS token
INDEX_URL = "https://mrfstore.uhc.com/public-mrf/2026-05-01/2026-05-01_Gruhn-Guitars-Inc_index.json?sv=2024-11-04&ss=b&srt=sco&sp=rwlitfx&se=2030-02-16T17:39:32Z&st=2026-02-16T09:24:32Z&spr=https&sig=1PcuH99nzLXbiaxh2perZSJub%2FTbVC5CB1wc9Y%2BaU7s%3D"

def fetch_index(url, out_dir="files/index_file"):
    """Download the index file."""
    # make sure the output directory exists before we try to write anything
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    # pull the filename out of the url then strip off any query string params
    fname = url.rsplit("/", 1)[-1]
    # Remove query parameters from filename (e.g., ?sv=2024-11-04&...)
    fname = fname.split("?")[0]
    out = pathlib.Path(out_dir) / fname

    # if we already downloaded this file skip it, no need to fetch again
    if out.exists():
        print(f"Index already exists at {out}")
        return out

    print(f"Downloading index from {url}...")
    # stream the download in 1MB chunks and show a progress bar so we can see how its going
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total_size = int(r.headers.get("content-length", 0))
        with tqdm(total=total_size, unit="B", unit_scale=True) as pbar:
            with open(out, "wb") as f:
                for chunk in r.iter_content(1024 * 1024):
                    f.write(chunk)
                    pbar.update(len(chunk))

    print(f"Index downloaded: {out}")
    return out


def iter_in_network_locations(index_path, limit=1000):
    """Yield (plan_name, plan_id, location_url) tuples from the index."""
    seen = 0
    index_path_str = str(index_path)

    # Handle both .gz and .json files
    if index_path_str.endswith(".gz"):
        # streaming parse the gzip file with ijson so we dont load whole thing into memory
        with gzip.open(index_path, "rb") as f:
            for rs in ijson.items(f, "reporting_structure.item"):
                plans = rs.get("reporting_plans", [])
                for inf in rs.get("in_network_files", []):
                    # take first plan if multiple plans share the same file
                    p = plans[0] if plans else {}
                    yield (p.get("plan_name"), p.get("plan_id"), inf.get("location"))
                    seen += 1
                    # stop once we hit the limit so we dont process thousands of files
                    if seen >= limit:
                        return
    else:
        # Plain JSON file - load it fully since plain json cant stream with ijson here
        with open(index_path, "r") as f:
            data = json.load(f)
            for rs in data.get("reporting_structure", []):
                plans = rs.get("reporting_plans", [])
                for inf in rs.get("in_network_files", []):
                    p = plans[0] if plans else {}
                    yield (p.get("plan_name"), p.get("plan_id"), inf.get("location"))
                    seen += 1
                    if seen >= limit:
                        return


def save_manifest(index_path, out_csv="files/index_file/manifest.csv", limit=1000):
    """Save the first N plan URLs to a CSV worklist."""
    pathlib.Path(out_csv).parent.mkdir(parents=True, exist_ok=True)

    # iterate through the index and write each plan entry as a row in the csv
    # this manifest is what we use later to drive the actual downloads
    with open(out_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["plan_name", "plan_id", "location"])
        for plan_name, plan_id, location in iter_in_network_locations(index_path, limit):
            writer.writerow([plan_name, plan_id, location])

    print(f"Manifest saved: {out_csv}")


def get_sizes(manifest_csv, out_csv="files/index_file/manifest_sized.csv"):
    """Fetch Content-Length for each file, save sorted by size."""
    # read all the rows from the manifest first so we know how many files to check
    rows = []
    with open(manifest_csv, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Fetching file sizes for {len(rows)} files...")
    sizes = []
    for row in tqdm(rows):
        plan_name, plan_id, url = row["plan_name"], row["plan_id"], row["location"]
        try:
            # use a HEAD request so we only get the headers, not the whole file
            h = requests.head(url, allow_redirects=True, timeout=20)
            size = int(h.headers.get("Content-Length", 0))
            sizes.append((plan_name, plan_id, url, size))
        except Exception as e:
            # if the size check fails just mark it as -1 so we can still include it
            print(f"  Error fetching size for {plan_id}: {e}")
            sizes.append((plan_name, plan_id, url, -1))

    # Sort by size ascending (smallest first)
    # processing smallest files first is useful for testing and to fail fast
    sizes.sort(key=lambda x: x[3])

    with open(out_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["plan_name", "plan_id", "location", "size_bytes"])
        writer.writerows(sizes)

    print(f"Manifest with sizes saved: {out_csv}")
    # print total compressed size so we have a rough idea of storage needs
    total_gb = sum(s[3] for s in sizes if s[3] > 0) / (1024**3)
    print(f"Total size of all files: {total_gb:.1f} GB (compressed)")


def download_one(url, out_dir="files/network_files", max_size_gb=1.0, max_unzip_size_gb=3.0):
    """Download file from URL and decompress it"""
    import logging
    logger = logging.getLogger(__name__)

    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    # strip query string from url to get a clean filename we can save to disk
    name = url.rsplit("/", 1)[-1].split("?")[0]
    gz_path = pathlib.Path(out_dir) / name
    json_path = gz_path.with_suffix("")  # Remove .gz to get .json

    # convert gb limits to bytes once upfront so we can compare directly
    max_size_bytes = max_size_gb * 1024**3
    max_unzip_bytes = max_unzip_size_gb * 1024**3

    # If already unzipped, skip - dont waste bandwidth downloading something we already have
    if json_path.exists() and json_path.stat().st_size > 0:
        logger.info(f"[DOWNLOAD] Skipped (already exists): {name}")
        return json_path, "skipped (already unzipped)"

    try:
        logger.info(f"[DOWNLOAD] Starting: {name}")

        # Check file size before downloading
        # we need to send a browser User-Agent or the server will reject the request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        logger.info(f"[DOWNLOAD] Checking size...")
        head_resp = requests.head(url, allow_redirects=True, timeout=20, headers=headers)
        file_size = int(head_resp.headers.get("Content-Length", 0))
        file_size_mb = file_size / (1024**2)
        logger.info(f"[DOWNLOAD] Remote size: {file_size_mb:.1f} MB")

        # bail out early if the compressed file is already over our size limit
        if file_size > max_size_bytes:
            logger.warning(f"[DOWNLOAD] Skipped (too large): {file_size_mb:.1f} MB > {max_size_gb*1024:.0f} MB limit")
            return None, f"skipped (compressed file too large: {file_size_mb:.1f} MB > {max_size_gb*1024:.0f} MB limit)"

        # Download with browser headers (server blocks requests without User-Agent)
        # write to a .part file first then rename so we dont leave broken files around
        logger.info(f"[DOWNLOAD] Downloading {file_size_mb:.1f} MB...")
        with requests.get(url, stream=True, timeout=120, headers=headers) as r:
            r.raise_for_status()
            tmp = gz_path.with_suffix(gz_path.suffix + ".part")
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(8 * 1024 * 1024):
                    f.write(chunk)
            tmp.rename(gz_path)

        logger.info(f"[DOWNLOAD] Download complete, decompressing...")

        try:
            logger.info(f"[DECOMPRESS] Starting decompression...")
            max_decompress_bytes = max_unzip_bytes

            # decompress in chunks and track bytes written so we can abort if its too big
            with gzip.open(gz_path, "rb") as f_in:
                with open(json_path, "wb") as f_out:
                    chunk_size = 8 * 1024 * 1024
                    bytes_written = 0
                    while True:
                        chunk = f_in.read(chunk_size)
                        if not chunk:
                            break
                        bytes_written += len(chunk)

                        # if uncompressed size exceeds limit, stop and delete both files
                        if bytes_written > max_decompress_bytes:
                            logger.warning(f"[DECOMPRESS] File too large, stopping")
                            f_out.close()
                            json_path.unlink()
                            gz_path.unlink()
                            return None, f"skipped (decompressed file too large: {bytes_written / (1024**3):.2f} GB > {max_unzip_size_gb} GB limit)"

                        f_out.write(chunk)

                    logger.info(f"[DECOMPRESS] Done: {bytes_written / (1024**2):.1f} MB")
            # delete the .gz after successful decompression to save disk space
            gz_path.unlink()
        except (gzip.BadGzipFile, OSError) as e:
            # Not actually gzipped - treat as plain JSON
            # some files just have .gz extension but are actually plain json
            logger.warning(f"[DECOMPRESS] Not gzipped ({e}), treating as plain JSON")
            gz_path.rename(json_path)

        # Check uncompressed file size - do a final check even after decompression
        unzip_size = json_path.stat().st_size
        unzip_size_gb = unzip_size / (1024**3)
        logger.info(f"[DOWNLOAD] Uncompressed size: {unzip_size_gb:.3f} GB")

        if unzip_size > max_unzip_bytes:
            logger.warning(f"[DOWNLOAD] Skipped (uncompressed too large): {unzip_size_gb:.1f} GB > {max_unzip_size_gb} GB")
            json_path.unlink()
            return None, f"skipped (uncompressed file too large: {unzip_size_gb:.1f} GB > {max_unzip_size_gb} GB limit)"

        logger.info(f"[DOWNLOAD] Success: {name}")
        return json_path, "ok (downloaded)"
    except Exception as e:
        # clean up any partial files on error so they dont interfere with next run
        logger.exception(f"[DOWNLOAD] Error: {e}")
        if gz_path.exists():
            gz_path.unlink()
        if json_path.exists():
            json_path.unlink()
        return None, f"error: {e}"


def download_many(manifest_csv, workers=4):
    """Download files from manifest, logging status. Files are unzipped and .gz deleted."""
    import concurrent.futures as cf

    # load the whole manifest into memory so we can submit all jobs at once
    rows = []
    with open(manifest_csv, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Download in order of size
    # create the log file before starting so we have a header even if things fail early
    log_csv = "files/index_file/download_log.csv"
    pathlib.Path(log_csv).parent.mkdir(parents=True, exist_ok=True)
    with open(log_csv, "w", newline="") as f:
        f.write("plan_id,status,path\n")

    print(f"Downloading {len(rows)} files with {workers} workers...")
    print(f"Files will be unzipped and .gz deleted. Only .json kept temporarily.")
    # use a thread pool to download multiple files in parallel
    # each thread calls download_one for its assigned url
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(download_one, r["location"]): r for r in rows}
        # as each future completes, write its result to the log immediately
        for fu in tqdm(cf.as_completed(futs), total=len(futs)):
            row = futs[fu]
            try:
                out, status = fu.result()
                with open(log_csv, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([row["plan_id"], status, out or ""])
            except Exception as e:
                # catch any unexpected errors so one bad file doesnt kill the whole run
                with open(log_csv, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([row["plan_id"], f"failed: {e}", ""])


# Note: download.py is now used by pipeline.py
# The download_one() function is called by the master pipeline
# Keep command-line interface for manual testing if needed
if __name__ == "__main__":
    import sys
    print("This module is used by pipeline.py. Run: python src/pipeline.py")
