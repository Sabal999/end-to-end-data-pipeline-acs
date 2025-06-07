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
        logger.info(f"Building and writing {dim_table}...")
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
    logger.info("Populating FACT table fact_population...")

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
    logger.info("Building intermediate MART int_population_enriched...")

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
                WHEN age < 16 THEN 'child'
                WHEN age < 18 THEN 'adolescent'
                WHEN age < 30 THEN 'young_adult'
                WHEN age < 65 THEN 'adult'
                ELSE 'senior'
            END AS age_group,
            CASE
                WHEN employment = 'employed' AND hrs_work > 0 AND income > 0 THEN true
                ELSE false
            END AS paid_worker,
            CASE
                WHEN income < 30000 THEN 'very low'
                WHEN income < 50000 THEN 'low'
                WHEN income < 100000 THEN 'mid'
                WHEN income < 200000 THEN 'high'
                WHEN income >= 200000 THEN 'very high'
                ELSE NULL
            END AS income_category,
            PERCENT_RANK() OVER (PARTITION BY gender, employment ORDER BY hrs_work) AS hrs_work_percentile
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
    logger.info("Building DATA MARTS...")

    # Define UDF 1
    def compute_commute_stress_score(time_to_work: float, hrs_work: float, age_group: str) -> float:
        """
        Computes a commute-related stress score.

        Logic idea:
        - Long commute time → increases stress.
        - Long working hours → increases stress → tiredness amplifies commute stress.
        - Seniors → higher stress from same commute.
        - Gender → optional modifier.

        Output:
        - Float score → higher = more stress.
        """

        # Base commute stress → exponential scaling
        commute_stress = min(time_to_work / 60.0, 1.0) ** 2 * 50  # 0-50 stress points

        # Work hours modifier → more hours → amplifies commute stress
        hrs_modifier = min(hrs_work / 60.0, 1.0) * 0.5 + 1.0  # multiplier ~1.0-1.5

        # Age modifier
        if age_group == 'senior':
            age_modifier = 1.5
        elif age_group == 'child':  # should not happen for paid_worker
            age_modifier = 1.0
        elif age_group == 'young_adult':
            age_modifier = 0.9
        else:  # 'adult'
            age_modifier = 1.0

        # Total stress score
        stress_score = commute_stress * hrs_modifier * age_modifier

        return float(stress_score)

    spark.udf.register("compute_employment_stress_score", compute_commute_stress_score, DoubleType())

    # Define UDF 2
    def compute_employment_stress_score(income: float, hrs_work: float, age_group: str) -> float:
        """
        Computes an employment-related stress score.

        Logic idea:
        - High working hours → increases stress.
        - Low income → increases stress.
        - Seniors → more stress for same conditions.
        - Gender → optional modifier (you may model different stress sensitivity by gender if desired).

        Output:
        - Float score → higher = more stress.
        - Normalize to e.g. [0.0, 100.0] or any scale you like.
        """

        # Base stress from working hours → linear effect
        hrs_stress = min(hrs_work / 60.0, 1.0) * 40  # 0-40 stress points

        # Income stress → inverse of income
        # Example: income < 20000 → high stress; income > 100000 → low stress
        if income <= 0:
            income_stress = 40.0
        elif income < 20000:
            income_stress = 30.0
        elif income < 50000:
            income_stress = 20.0
        elif income < 100000:
            income_stress = 10.0
        else:
            income_stress = 5.0

        # Age modifier
        if age_group == 'senior':
            age_modifier = 1.3
        elif age_group == 'child':  # should not happen for paid_worker, but be safe
            age_modifier = 1.0
        elif age_group == 'young_adult':
            age_modifier = 0.9
        else:  # 'adult'
            age_modifier = 1.0


        # Total stress score
        stress_score = (hrs_stress + income_stress) * age_modifier 

        return float(stress_score)

    spark.udf.register("compute_commute_stress_score", compute_employment_stress_score, DoubleType())

    # Mart 1: Average income
    dmart_income_avg = spark.sql("""
        SELECT gender, edu, race, age_group, ROUND(AVG(income)) AS average_income
        FROM int_population_enriched
        WHERE paid_worker = true
        GROUP BY gender, edu, race, age_group
        ORDER BY average_income;
        """)

    dmart_income_avg.write.jdbc(
        url=POSTGRES_URL,
        table="dmart_avg",
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )
    logger.info("Built dmart_avg_income.")

    # Mart 2: Average hours worked
    dmart_hrs_work = spark.sql("""
        SELECT gender, edu, race, age_group, ROUND(AVG(hrs_work), 1) AS average_hours
        FROM int_population_enriched
        WHERE paid_worker = true
        GROUP BY gender, edu, race, age_group
        ORDER BY average_hours DESC;
        """)

    dmart_hrs_work.write.jdbc(
        url=POSTGRES_URL,
        table="dmart_avg_hrs_work",
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )
    logger.info("Built dmart_avg_hrs_work.")

    # Mart 3: Average commute stress score
    dmart_commute_stress_score = spark.sql("""
        SELECT gender, 
               edu, 
               race, 
               age_group, 
               ROUND(AVG(compute_commute_stress_score(time_to_work, hrs_work, age_group)), 2) as average_commute_stress
        FROM int_population_enriched
        WHERE paid_worker = true AND time_to_work > 0
        GROUP BY gender, edu, race, age_group
        ORDER BY average_commute_stress DESC;                                       
        """)

    dmart_commute_stress_score.write.jdbc(
        url=POSTGRES_URL,
        table="dmart_avg_commute_stress_score",
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )
    logger.info("Built dmart_avg_commute_stress_score.")

    # Mart 4: Percentage of children by race
    dmart_pct_children = spark.sql("""                                   
        SELECT 
          race,
          ROUND(100.0 * SUM(CASE 
                              WHEN age_group = 'child' 
                              THEN 1 ELSE 0 
                              END) / COUNT(*), 2)  AS child_percentage
        FROM int_population_enriched
        GROUP BY race
        ORDER BY child_percentage DESC;
    """)

    dmart_pct_children.write.jdbc(
        url=POSTGRES_URL,
        table="dmart_pct_children_by_race",
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )
    logger.info("Built dmart_pct_children_by_race.")

    # Mart 5: Average employment stress score
    dmart_employment_stress_score = spark.sql("""
        SELECT gender, 
               edu, 
               race, 
               age_group, 
               ROUND(AVG(compute_employment_stress_score(income, hrs_work, age_group)), 2) as average_employment_stress
        FROM int_population_enriched
        WHERE paid_worker = true AND time_to_work > 0
        GROUP BY gender, edu, race, age_group 
        ORDER BY average_employment_stress DESC;

    """)

    dmart_employment_stress_score.write.jdbc(
        url=POSTGRES_URL,
        table="dmart_avg_employment_stress_score",
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )
    logger.info("Built dmart_avg_employment_stress_score.")



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
