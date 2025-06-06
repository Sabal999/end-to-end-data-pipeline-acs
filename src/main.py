import logging
from logger import configure_logging
import asyncio
from pathlib import Path
import pandas as pd
from simulate_ingestion import simulate_ingestion
from cleaning import a_preprocess, df_drop_duplicates_merged_df
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

configure_logging()
logger = logging.getLogger(__name__)

BASE_DATA_DIR = Path("data")
DATA_LAKE_DIR = BASE_DATA_DIR / "data_lake"
DATA_SOURCES_DIR = BASE_DATA_DIR / "data_sources"
STAGING_AREA_DIR = BASE_DATA_DIR / "staging_area"


async def main():
    simulate_ingestion(DATA_SOURCES_DIR, DATA_LAKE_DIR)
    # gather files from data lake
    csv_file_paths = list(DATA_LAKE_DIR.glob("*.csv"))
    # clean them asynchronously
    logger.info("beginning asynchronous cleaning")
    tasks = [
        a_preprocess(STAGING_AREA_DIR, csv_file_path)
        for csv_file_path in csv_file_paths
    ]
    await asyncio.gather(*tasks)
    logger.info(
        "all files have been cleaned and stored in the staging area"
    )
    df = pd.concat(
        [
            pd.read_pickle(path)
            for path in Path(STAGING_AREA_DIR).glob("*.pkl")
        ],
        ignore_index=True,
    )
    df = df_drop_duplicates_merged_df(df)

    # spark_df = apply_transformations(df)
    # summary1, summary2 = generate_summary_tables(spark_df)

    # notebook df export
    df.to_pickle("debug.pkl")


if __name__ == "__main__":
    asyncio.run(main())
