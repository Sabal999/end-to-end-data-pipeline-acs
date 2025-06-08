"""
Database initialization module.

Drops and recreates:
- acs_dataset database
- dim tables
- fact table
- Adds constraints and FK references.

Used at the start of each pipeline run to ensure a clean database.
"""

import psycopg2
import logging
from logger import configure_logging
import os
import time

configure_logging()
logger = logging.getLogger(__name__ + ".py")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Password21!!!")


def wait_for_pg_to_be_ready():
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=5432,
                dbname="postgres",
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            conn.close()
            logger.info("Postgres is ready!")
            break
        except psycopg2.OperationalError:
            logger.info("Waiting for Postgres...")
            time.sleep(2)


def create_database_and_tables():
    # Step 1: connect to system database → create/drop acs_dataset

    sys_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=5432,
        dbname="postgres",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    sys_conn.autocommit = True
    sys_cur = sys_conn.cursor()

    logger.info("Dropping and recreating database acs_dataset...")
    sys_cur.execute("DROP DATABASE IF EXISTS acs_dataset;")
    sys_cur.execute("CREATE DATABASE acs_dataset;")

    sys_cur.close()
    sys_conn.close()
    logger.info("Database acs_dataset created.")

    # Step 2: connect to acs_dataset database and create tables
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=5432,
        dbname="acs_dataset",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    cur = conn.cursor()

    # Create DIM tables
    dims = {
        "dim_employment": "employment_text",
        "dim_race": "race_text",
        "dim_gender": "gender_text",
        "dim_lang": "lang_text",
        "dim_education": "education_text",
        "dim_birth_qrtr": "birth_qrtr_text",
    }

    for dim_name, column_name in dims.items():
        logger.info(f"Creating table {dim_name}...")
        cur.execute(
            f"""
            DROP TABLE IF EXISTS {dim_name} CASCADE;
            CREATE TABLE {dim_name} (
                id SERIAL PRIMARY KEY,
                {column_name} TEXT UNIQUE
            );
        """
        )
        logger.info(f"Created table {dim_name}.")

    # Create FACT table using _id postfix for FKs
    logger.info("Creating table fact_population...")
    cur.execute(
        """
        DROP TABLE IF EXISTS fact_population CASCADE;
        CREATE TABLE fact_population (
            id SERIAL PRIMARY KEY,
            employment_id INTEGER REFERENCES dim_employment(id),
            race_id INTEGER REFERENCES dim_race(id),
            gender_id INTEGER REFERENCES dim_gender(id),
            lang_id INTEGER REFERENCES dim_lang(id),
            education_id INTEGER REFERENCES dim_education(id),
            birth_qrtr_id INTEGER REFERENCES dim_birth_qrtr(id),
            income DOUBLE PRECISION,
            hrs_work DOUBLE PRECISION,
            age INTEGER,
            time_to_work DOUBLE PRECISION,
            citizen BOOLEAN,
            married BOOLEAN,
            disability BOOLEAN,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """
    )
    logger.info("Created table fact_population.")

    # Create indexes on fact table FK's
    cur.execute(
        "CREATE INDEX fact_employment_id_idx ON fact_population (employment_id);"
    )
    cur.execute(
        "CREATE INDEX fact_race_id_idx ON fact_population (race_id);"
    )
    cur.execute(
        "CREATE INDEX fact_gender_id_idx ON fact_population (gender_id);"
    )
    cur.execute(
        "CREATE INDEX fact_lang_id_idx ON fact_population (lang_id);"
    )
    cur.execute(
        "CREATE INDEX fact_education_id_idx ON fact_population (education_id);"
    )
    cur.execute(
        "CREATE INDEX fact_birth_qrtr_id_idx ON fact_population (birth_qrtr_id);"
    )
    logger.info("Creating FK indexes")

    # Done
    conn.commit()
    cur.close()
    conn.close()

    logger.info("All DIM + FACT tables created successfully.")
