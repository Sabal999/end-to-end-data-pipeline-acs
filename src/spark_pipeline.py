import logging
from logger import configure_logging
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
import pandas as pd


configure_logging()
logger = logging.getLogger(__name__ + '.py')

POSTGRES_URL = "jdbc:postgresql://localhost:5432/acs_dataset"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "Password21!!!",
    "driver": "org.postgresql.Driver",
}

def get_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars", "jars/postgresql-42.7.3.jar"
    ).getOrCreate()
    return spark

def register_cleaned_df_as_view(spark: SparkSession, df_cleaned: pd.DataFrame):
    sdf = spark.createDataFrame(df_cleaned)
    sdf.createOrReplaceTempView("acs_raw_cleaned")

    logger.info("Building acs_cleaned view with final_clean_nan logic in SparkSQL...")

    clean_query = """
        CREATE OR REPLACE TEMP VIEW acs_cleaned AS
        SELECT
            CASE WHEN isnan(income) THEN NULL ELSE income END AS income,
            CASE WHEN isnan(hrs_work) THEN NULL ELSE hrs_work END AS hrs_work,
            CASE WHEN isnan(time_to_work) THEN NULL ELSE time_to_work END AS time_to_work,
            CASE WHEN birth_qrtr = 'NaN' THEN NULL ELSE birth_qrtr END AS birth_qrtr,
            CASE WHEN employment = 'NaN' THEN NULL ELSE employment END AS employment,
            CASE WHEN race = 'NaN' THEN NULL ELSE race END AS race,
            CASE WHEN gender = 'NaN' THEN NULL ELSE gender END AS gender,
            CASE WHEN lang = 'NaN' THEN NULL ELSE lang END AS lang,
            CASE WHEN edu = 'NaN' THEN NULL ELSE edu END AS edu,
            citizen,
            married,
            disability,
            age
        FROM acs_raw_cleaned
    """

    spark.sql(clean_query)
    spark.catalog.cacheTable("acs_cleaned")
    logger.info("Registered and cached acs_cleaned as SparkSQL temp view (with NaN cleaning).")

def populate_dims(spark: SparkSession):
    logger.info("Populating DIM tables (pure SparkSQL)...")

    dims = [
        ("dim_employment", "employment", "employment_text"),
        ("dim_race", "race", "race_text"),
        ("dim_gender", "gender", "gender_text"),
        ("dim_lang", "lang", "lang_text"),
        ("dim_education", "edu", "education_text"),
        ("dim_birth_qrtr", "birth_qrtr", "birth_qrtr_text"),
    ]

    for dim_table, source_column, target_column in dims:
        logger.info(f"Building and writing {dim_table} using SparkSQL...")
        query = f"""
            SELECT DISTINCT {source_column} AS {target_column}
            FROM acs_cleaned
            WHERE {source_column} IS NOT NULL
        """
        sdf_dim = spark.sql(query)

        sdf_dim.write.jdbc(
            url=POSTGRES_URL,
            table=dim_table,
            mode="append",  # make use of the surrogate primary key defined in DDL
            properties=POSTGRES_PROPERTIES,
        )
        logger.info(f"Finished writing {dim_table}.")

def populate_fact(spark: SparkSession):
    logger.info("Populating FACT table fact_population (pure SparkSQL)...")

    dim_tables = [
        "dim_employment",
        "dim_race",
        "dim_gender",
        "dim_lang",
        "dim_education",
        "dim_birth_qrtr",
    ]

    # Register DIM tables as views
    for dim_table in dim_tables:
        dim_df = spark.read.jdbc(
            url=POSTGRES_URL,
            table=dim_table,
            properties=POSTGRES_PROPERTIES,
        )
        dim_df.createOrReplaceTempView(dim_table)
        logger.info(f"Registered {dim_table} as SparkSQL temp view.")

    # Build FACT via SparkSQL JOINs
    fact_query = """
        SELECT
            dim_employment.id AS employment_id,
            dim_race.id AS race_id,
            dim_gender.id AS gender_id,
            dim_lang.id AS lang_id,
            dim_education.id AS education_id,
            dim_birth_qrtr.id AS birth_qrtr_id,
            acs_cleaned.income,
            acs_cleaned.hrs_work,
            acs_cleaned.age,
            acs_cleaned.time_to_work,
            acs_cleaned.citizen,
            acs_cleaned.married,
            acs_cleaned.disability
        FROM acs_cleaned
        LEFT JOIN dim_employment ON acs_cleaned.employment = dim_employment.employment_text
        LEFT JOIN dim_race ON acs_cleaned.race = dim_race.race_text
        LEFT JOIN dim_gender ON acs_cleaned.gender = dim_gender.gender_text
        LEFT JOIN dim_lang ON acs_cleaned.lang = dim_lang.lang_text
        LEFT JOIN dim_education ON acs_cleaned.edu = dim_education.education_text
        LEFT JOIN dim_birth_qrtr ON acs_cleaned.birth_qrtr = dim_birth_qrtr.birth_qrtr_text
    """

    logger.info("Running SparkSQL query to build FACT table...")
    fact_df = spark.sql(fact_query)

    fact_df.write.jdbc(
        url=POSTGRES_URL,
        table="fact_population",
        mode="append",  # make use of the surrogate primary key defined in DDL
        properties=POSTGRES_PROPERTIES,
    )
    logger.info("Finished populating FACT table fact_population.")

def build_intermediate_mart(spark: SparkSession):
    logger.info("Building intermediate MART int_population_enriched (pure SparkSQL)...")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW int_population_enriched AS
        SELECT
            employment,
            income,
            hrs_work,
            age,
            time_to_work,
            citizen,
            married,
            disability,
            gender,
            race,
            lang,
            edu,
            birth_qrtr,
            -- Derived columns:
            CASE
                WHEN age < 18 THEN 'child'
                WHEN age >= 18 AND age < 30 THEN 'young_adult'
                WHEN age >= 30 AND age < 65 THEN 'adult'
                ELSE 'senior'
            END AS categorize_age,
            CASE
                WHEN employment = 'employed' AND hrs_work > 0 AND income > 0 THEN true
                ELSE false
            END AS paid_worker,
            CASE
                WHEN income < 30000 THEN 'low'
                WHEN income < 75000 THEN 'mid'
                ELSE 'high'
            END AS income_category
        FROM acs_cleaned
    """)

    logger.info("Writing int_population_enriched to Postgres...")

    sdf_int_mart = spark.sql("SELECT * FROM int_population_enriched")
    sdf_int_mart.write.jdbc(
        url=POSTGRES_URL,
        table="int_population_enriched",
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )
    logger.info("Finished writing int_population_enriched to Postgres.")


def build_final_marts(spark: SparkSession):
    logger.info("Building DATA MARTS (pure SparkSQL)...")

    # Define UDF 1: compute_income_percentile
    def compute_employment_stress_score(income: float, hrs_work: float, categorize_age: str, gender: str) -> float:
        """
        Final UDF: compute_employment_stress_score
        Inputs:

        hrs_work → hours worked per week.

        income → income.

        categorize_age → age group.

        gender → gender.

        Purpose:

        Model an employment-related stress score for paid employed workers:

        Long working hours → higher stress.

        Low income → higher stress.

        Seniors → more stress for same conditions.

        Gender factor (optional) → you can add this nuance if you want.
        """
        return 0.0  # placeholder

    spark.udf.register("compute_employment_stress_score", compute_employment_stress_score, DoubleType())

    # Define UDF 2: compute_commute_score
    def compute_commute_stress_score(time_to_work: float, hrs_work: float, categorize_age: str, gender: str) -> float:
        """
        UDF 2 (revised): compute_commute_stress_score(time_to_work, hrs_work, categorize_age, gender)
        👉 Idea:

        Commute "stress" is not only about time to work → it also depends on:
        How many hours the person works per week (hrs_work).
        Whether they are young/old (commute may be harder for some age groups).
        Gender → could introduce a social factor → e.g. stress for women working long hours with long commutes.
        What it should do:
        Combine all these inputs into a non-trivial stress score → not easily written in plain SQL.

        Input parameters:

        Param	Meaning
        time_to_work	Commute time in minutes.
        hrs_work	Hours worked per week.
        categorize_age	Age group.
        gender	Gender.
        Suggested logic (you will implement it!):
        Base score starts from commute time.

        Modifier based on hrs_work:
        Very long working hours → increases stress.
        Short hours → lower stress.
        Modifier based on categorize_age:
        Seniors → more stress from same commute.
        Young adults → more resilient.
        Modifier based on gender (if you want to model it).
        ✅ This is very hard to write purely in SQL, because you would need many nested CASE WHEN and interactions → perfect for UDF.
        """
        return 0.0  # placeholder

    spark.udf.register("compute_commute_stress_score", compute_commute_stress_score, DoubleType())

    # Mart 1: Average income by education
    # Description: For each education level (edu), compute the average income.
    # Filter: Only include rows where paid_worker = true.
    dmart_income_avg = spark.sql("""
        -- TODO: Write SQL to compute average income by edu, filtered on paid_worker = true

        
    """)

    dmart_income_avg.write.jdbc(
        url=POSTGRES_URL,
        table="dmart_avg_income_by_education",
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )
    logger.info("Built dmart_avg_income_by_education.")

    # Mart 2: Average hours worked by gender
    # Description: For each gender, compute the average number of hours worked.
    # Filter: Only include rows where paid_worker = true.
    dmart_hrs_work = spark.sql("""
        -- TODO: Write SQL to compute average hrs_work by gender, filtered on paid_worker = true
    """)

    dmart_hrs_work.write.jdbc(
        url=POSTGRES_URL,
        table="dmart_avg_hrs_work_by_gender",
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )
    logger.info("Built dmart_avg_hrs_work_by_gender.")

    # Mart 3: Average commute stress score by gender
    # Description:
    # For each gender, compute the average commute stress score.
    # The UDF compute_commute_stress_score should combine:
    # - commute time (time_to_work)
    # - hours worked (hrs_work)
    # - categorize_age
    # - gender
    # Filter: Only include rows where paid_worker = true.
    dmart_commute_stress_score = spark.sql("""
        -- TODO: Write SQL to compute avg commute stress score by gender, using UDF compute_commute_stress_score, filtered on paid_worker = true
    """)

    dmart_commute_stress_score.write.jdbc(
        url=POSTGRES_URL,
        table="dmart_avg_commute_stress_score_by_gender",
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )
    logger.info("Built dmart_avg_commute_stress_score_by_gender.")

    # Mart 4: Percentage of children by race + gender
    # Description: For each combination of race and gender, compute the percentage of population that are children (categorize_age = 'child').
    # No filter — include all rows.
    dmart_pct_children = spark.sql("""
        -- TODO: Write SQL to compute percentage of children by race + gender (categorize_age = 'child')
    """)

    dmart_pct_children.write.jdbc(
        url=POSTGRES_URL,
        table="dmart_pct_children_by_race_gender",
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )
    logger.info("Built dmart_pct_children_by_race_gender.")

    # Mart 5: Average employment stress score by categorize_age and gender
    # Description:
    # For each combination of categorize_age and gender, compute the average employment stress score.
    # The UDF compute_employment_stress_score should combine:
    # - hrs_work
    # - income
    # - categorize_age
    # - gender
    # Filter: Only include rows where paid_worker = true.
    dmart_employment_stress_score = spark.sql("""
        -- TODO: Write SQL to compute avg employment stress score by categorize_age and gender
        -- Use UDF compute_employment_stress_score(hrs_work, income, categorize_age, gender)
        -- Filter rows: paid_worker = true
        -- Group by: categorize_age, gender
    """)

    dmart_employment_stress_score.write.jdbc(
        url=POSTGRES_URL,
        table="dmart_avg_employment_stress_score_by_age_gender",
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )
    logger.info("Built dmart_avg_employment_stress_score_by_age_gender.")



def orchestrate_pipeline(df_cleaned: pd.DataFrame):
    logger.info("Starting Spark pipeline orchestration...")

    spark = get_spark_session()
    register_cleaned_df_as_view(spark, df_cleaned)
    populate_dims(spark)
    populate_fact(spark)
    build_intermediate_mart(spark)
    build_final_marts(spark)

    spark.stop()
    logger.info("Spark pipeline orchestration complete. Spark session stopped.")
