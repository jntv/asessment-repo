import os
import snowflake.connector
from dotenv import load_dotenv
from config import STORAGE_MODE

load_dotenv()

def setup_s3_stage(cur):
    """Create S3 external stage in Snowflake"""
    from config import SF_S3_ROLE, S3_BUCKET, S3_PREFIX

    if not SF_S3_ROLE:
        raise ValueError("SF_S3_ROLE not set in .env. Required for S3 external stage.")

    # Create or replace S3 stage
    stage_sql = f"""
    CREATE OR REPLACE STAGE s3_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{S3_BUCKET}/{S3_PREFIX}'
        FILE_FORMAT = (FORMAT_NAME = pq_fmt);
    """

    try:
        cur.execute(stage_sql)
        print("[OK] S3 stage ready")
    except Exception as e:
        # Stage might already exist; verify with a list command
        try:
            cur.execute("LIST @s3_stage LIMIT 1")
            print("[OK] S3 stage exists and is accessible")
        except Exception as verify_error:
            raise ValueError(f"Failed to create/access S3 stage: {e}") from verify_error


def load_snowflake():
    """Load parquet files from S3 into Snowflake"""
    conn = snowflake.connector.connect(
        user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"],
        account=os.environ["SF_ACCOUNT"],
        warehouse=os.environ["SF_WAREHOUSE"],
        database=os.environ["SF_DATABASE"],
        schema=os.environ["SF_SCHEMA"],
        role="SYSADMIN",
    )
    cur = conn.cursor()

    if STORAGE_MODE == "S3":
        print("Loading parquet files from S3 to Snowflake...")
        print(f"S3 Bucket: {os.environ['S3_BUCKET']}")
        print(f"S3 Prefix: {os.environ.get('S3_PREFIX', 'parquet/')}")
        # Setup S3 stage
        setup_s3_stage(cur)
        stage_name = "@s3_stage"
    else:
        print("Loading parquet files from local storage to Snowflake...")
        # For LOCAL mode, we'll PUT files from data/parquet/ to internal stage
        from pathlib import Path
        parquet_dir = Path("data/parquet")
        parquet_files = list(parquet_dir.glob("*.parquet"))

        if not parquet_files:
            print("No parquet files found in data/parquet/")
            conn.close()
            return

        print(f"Found {len(parquet_files)} parquet files to upload")
        print("Uploading to internal stage...")
        for pf in parquet_files:
            cur.execute(f"PUT file://{pf.absolute()} @pq_stage")
            print(f"  [OK] {pf.name}")

        stage_name = "@pq_stage"

    # COPY all files from stage to table
    print(f"Copying from {stage_name} to table...")
    copy_sql = f"""
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
      FROM {stage_name}
    )
    FILE_FORMAT = (FORMAT_NAME = pq_fmt)
    ON_ERROR = 'CONTINUE';
    """
    cur.execute(copy_sql)
    print("COPY completed")

    # Get load stats
    cur.execute("SELECT COUNT(*) FROM rates_raw")
    row_count = cur.fetchone()[0]
    print(f"Total rows loaded: {row_count:,}")

    conn.close()


if __name__ == "__main__":
    load_snowflake()
