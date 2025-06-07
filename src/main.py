"""
Main orchestration script for the ACS pipeline.

Steps:
1. Clean up output folders.
2. Simulate ingestion of CSV chunks.
3. Clean and deduplicate the data.
4. Initialize the database (dim, fact).
5. Run Spark pipeline (populate dims, fact, intermediate mart, final marts).
"""

import logging
from logger import configure_logging
import asyncio
from pathlib import Path
import pandas as pd
from simulate_ingestion import simulate_ingestion
from cleaning import a_preprocess, df_drop_duplicates_merged_df
from clean_up import multi_cleanup
from db_init import create_database_and_tables
from spark_pipeline import orchestrate_pipeline

configure_logging()
logger = logging.getLogger(__name__ + ".py")

BASE_DATA_DIR = Path("data")
DATA_LAKE_DIR = BASE_DATA_DIR / "data_lake"
DATA_SOURCES_DIR = BASE_DATA_DIR / "data_sources"
STAGING_AREA_DIR = BASE_DATA_DIR / "staging_area"


async def main():
    # make sure data lake and staging area are empty
    multi_cleanup(DATA_LAKE_DIR, STAGING_AREA_DIR)
    # We only have one source, this simulates multiple ones by splitting the csv.
    simulate_ingestion(DATA_SOURCES_DIR, DATA_LAKE_DIR)
    # gather chunks (split csv's) from data lake
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
    # retrieve all dataframes (pickles) from staging area and concatenate them
    df = pd.concat(
        [
            pd.read_pickle(path)
            for path in Path(STAGING_AREA_DIR).glob("*.pkl")
        ],
        ignore_index=True,
    )
    # drop duplicates in the concatenated dataframe
    df = df_drop_duplicates_merged_df(df)

    # prepares our database by creating empty fact and dim tables
    create_database_and_tables()
    # Start main spark pipeline (Transform and Load)
    orchestrate_pipeline(df)

    logger.info(
        "Pipeline has ran successfully tables have been loaded to the DWH"
    )


if __name__ == "__main__":
    asyncio.run(main())
