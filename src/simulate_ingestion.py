import logging
from logger import configure_logging
import pandas as pd
import os
from pathlib import Path

configure_logging()
logger = logging.getLogger(__name__)

ENTRIES_PER_FILE = 100


def simulate_ingestion(data_source_path: Path, data_lake_pth: Path):
    logger.info("Beginning Ingestion")
    # retrieve sources
    sources = list(data_source_path.glob("*.csv"))
    # combine sources into a dataframe
    sources_aggregation_df = pd.concat(
        [pd.read_csv(source) for source in sources]
    )
    # split them into chunks for asynchronous cleaning
    split_dfs = [
        sources_aggregation_df.iloc[i : i + ENTRIES_PER_FILE, :]
        for i in range(
            0, len(sources_aggregation_df) - 1, ENTRIES_PER_FILE
        )
    ]
    df_length = len(split_dfs)
    os.makedirs(data_lake_pth, exist_ok=True)
    for i, sources_aggregation_df in enumerate(split_dfs):
        filename = f"{data_lake_pth}/acs{i+1}of{df_length}.csv"
        logger.info(f"Ingesting  {filename}")
        sources_aggregation_df.to_csv(filename, index=False)
    logger.info(f"{df_length} csv files have landed on the data lake")
    logger.info("Ingestion Complete.")
