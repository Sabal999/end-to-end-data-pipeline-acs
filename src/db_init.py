import psycopg2
import logging
from logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__ + '.py')

def create_database_and_tables():
    # Step 1 → connect to system database → create/drop acs_dataset
    sys_conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",
        user="postgres",
        password="Password21!!!"
    )
    sys_conn.autocommit = True
    sys_cur = sys_conn.cursor()

    logger.info("Dropping and recreating database acs_dataset...")
    sys_cur.execute("DROP DATABASE IF EXISTS acs_dataset;")
    sys_cur.execute("CREATE DATABASE acs_dataset;")

    sys_cur.close()
    sys_conn.close()
    logger.info("Database acs_dataset created.")

    # Step 2 → connect to acs_dataset → create tables
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="acs_dataset",
        user="postgres",
        password="Password21!!!"
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
        cur.execute(f"""
            DROP TABLE IF EXISTS {dim_name} CASCADE;
            CREATE TABLE {dim_name} (
                id SERIAL PRIMARY KEY,
                {column_name} TEXT UNIQUE
            );
        """)
        logger.info(f"Created table {dim_name}.")

    # Create FACT table → using _id postfix for FKs
    logger.info("Creating table fact_population...")
    cur.execute("""
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
            disability BOOLEAN
        );
    """)
    logger.info("Created table fact_population.")

    # Done
    conn.commit()
    cur.close()
    conn.close()

    logger.info("All DIM + FACT tables created successfully.")

