import logging
from logger import configure_logging
from transform import (
    apply_transformations_into_fact_population,
    generate_fact_paid_workers_table,
    generate_data_mart_work_summary_by_gender,
    generate_data_mart_children_percentage_by_race_gender,
    generate_data_mart_education_distribution_among_unemployed,
    generate_data_mart_income_above_median_by_edu,
    generate_data_mart_population_summary_by_age_group,
    generate_data_mart_income_summary_by_demo,
    generate_data_mart_unemployed_percentage_by_age_group_gender,
)
from pyspark.sql import DataFrame

configure_logging()
logger = logging.getLogger(__name__)

DATA_MART_PREFIX = "dmart"
FACT_PREFIX = "fact"


def orchestrate_table_creation(df_cleaned):
    # FACT TABLES
    fact_population_sdf = apply_transformations_into_fact_population(
        df_cleaned
    )
    save_sdf_to_postgres(
        fact_population_sdf, f"{FACT_PREFIX}_population"
    )

    fact_paid_workers = generate_fact_paid_workers_table(
        fact_population_sdf
    )
    save_sdf_to_postgres(
        fact_paid_workers, f"{FACT_PREFIX}_paid_workers"
    )

    # DATA MARTS
    save_sdf_to_postgres(
        generate_data_mart_income_above_median_by_edu(
            fact_paid_workers
        ),
        f"{DATA_MART_PREFIX}_income_above_median_by_edu",
    )

    save_sdf_to_postgres(
        generate_data_mart_work_summary_by_gender(fact_paid_workers),
        f"{DATA_MART_PREFIX}_summary_by_gender",
    )

    save_sdf_to_postgres(
        generate_data_mart_population_summary_by_age_group(
            fact_paid_workers
        ),
        f"{DATA_MART_PREFIX}_population_summary_by_age_group",
    )

    save_sdf_to_postgres(
        generate_data_mart_income_summary_by_demo(fact_paid_workers),
        f"{DATA_MART_PREFIX}_income_summary_by_demo",
    )

    save_sdf_to_postgres(
        generate_data_mart_children_percentage_by_race_gender(
            fact_population_sdf
        ),
        f"{DATA_MART_PREFIX}_children_percentage_by_race_gender",
    )

    save_sdf_to_postgres(
        generate_data_mart_unemployed_percentage_by_age_group_gender(
            fact_population_sdf
        ),
        f"{DATA_MART_PREFIX}_unemployed_percentage_by_age_group_gender",
    )

    save_sdf_to_postgres(
        generate_data_mart_education_distribution_among_unemployed(
            fact_population_sdf
        ),
        f"{DATA_MART_PREFIX}_education_distribution_among_unemployed",
    )


# Postgres connection settings
POSTGRES_URL = "jdbc:postgresql://localhost:5432/acs_dataset"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "Password21!!!",
    "driver": "org.postgresql.Driver",
}


def save_sdf_to_postgres(
    sdf: DataFrame, table_name: str, mode="overwrite"
):
    """
    Save Spark DataFrame to Postgres table.
    If table does not exist, it will be created automatically.
    mode: "overwrite" to replace, "append" to insert
    """
    logger.info(f"Saving table '{table_name}' to Postgres ({mode})...")
    sdf.write.jdbc(
        url=POSTGRES_URL,
        table=table_name,
        mode=mode,
        properties=POSTGRES_PROPERTIES,
    )
    logger.info(f"Table '{table_name}' saved.")
