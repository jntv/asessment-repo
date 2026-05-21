import csv
from pathlib import Path

progress_file = Path('files/index_file/progress.csv')

# read all progress rows into memory so we can modify and write them back
rows = []
with open(progress_file, 'r', newline='') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# Count what we're resetting
size_failures = 0
other_failures = 0

# Reset only size-related failures
# we only want to retry files that failed because of size limits, not actual broken files
for row in rows:
    if row['status'] == 'failed':
        error = row.get('error_message', '')
        if 'too large' in error or 'limit' in error:
            # This is a size limit failure - reset to pending so pipeline retries it
            row['status'] = 'pending'
            row['error_message'] = ''
            row['downloaded_at'] = ''
            row['parsed_at'] = ''
            size_failures += 1
        else:
            # leave other failures alone, those need manual investigation
            other_failures += 1

# Write back - overwrite the progress file with the updated statuses
with open(progress_file, 'w', newline='') as f:
    fieldnames = ['url', 'plan_name', 'plan_id', 'reporting_entity', 'status', 'downloaded_at', 'parsed_at', 'error_message']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"Reset {size_failures} size-related failures to pending")
print(f"Left {other_failures} other failures unchanged")
print(f"\nRun 'python src/pipeline.py' again to process them with new 1GB/3GB limits")
