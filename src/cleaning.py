"""
Cleaning module for ACS CSV chunks.

Functions:
- df_cleaner(): applies a sequence of cleaning functions to a pandas DataFrame.
- a_preprocess(): asynchronously reads a CSV file, cleans it, and stores a cleaned pickle.
- df_standardize_dtypes(): standardizes column types.
- reclassify_not_employed(): marks rows with hrs_work > 0 as 'employed'.
- simplify_birth_quarter(): maps full quarter names to q1-q4.
- set_zero_income_as_null(): sets income==0 to NULL (pd.NA).
- set_zero_commute_time_as_zero_for_employed(): sets missing commute time to 0 for employed workers.

This module is used in the pipeline to prepare clean data for Spark processing.
"""

import logging
from logger import configure_logging
import pandas as pd
import aiofiles
import os
from datetime import datetime
from io import StringIO
from pathlib import Path

configure_logging()
logger = logging.getLogger(__name__ + ".py")


def df_cleaner(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(df_standardize_dtypes)
        .pipe(reclassify_not_employed)
        .pipe(simplify_birth_quarter)
        .pipe(set_zero_commute_time_as_zero_for_employed)
    )


async def a_preprocess(output_dir: Path, file_path: Path):
    # Read CSV asynchronously
    async with aiofiles.open(file_path, mode="r") as f:
        content = await f.read()
    # Load CSV content into pandas DataFrame
    df = pd.read_csv(StringIO(content))

    # Process with your cleaning function
    clean_df = df_cleaner(df)

    # Create unique output filename
    base_name = os.path.basename(file_path)
    name = os.path.splitext(base_name)[0]
    now = datetime.now()
    timestamp = (
        now.strftime("%y_%m_%d_%H_%M_%S_")
        + f"{now.microsecond // 1000:03d}"
    )
    output_file = output_dir / f"{name}_{timestamp}.pkl"
    # Save cleaned DataFrame synchronously (pandas doesn't support async writes)
    clean_df.to_pickle(
        output_file,
    )
    logger.info(f"cleaned and stored: {output_file}")


def df_standardize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    for column in [
        "income",
        "hrs_work",
        "time_to_work",
    ]:
        df[column] = df[column].astype("float64")
    for column in [
        "employment",
        "race",
        "age",
        "gender",
        "lang",
        "edu",
    ]:
        df[column] = df[column].astype("object")
    for column in ["citizen", "married", "disability"]:
        df[column] = df[column].map({"yes": True, "no": False})
        df[column] = df[column].astype("bool")
    df["age"] = df["age"].astype("int64")
    return df


def reclassify_not_employed(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df["hrs_work"] > 0, "employment"] = "employed"
    return df


def simplify_birth_quarter(df: pd.DataFrame) -> pd.DataFrame:
    df["birth_qrtr"] = df["birth_qrtr"].map(
        {
            "jan thru mar": "q1",
            "apr thru jun": "q2",
            "jul thru sep": "q3",
            "oct thru dec": "q4",
        }
    )
    return df


def set_zero_income_as_null(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df["income"] == 0, "income"] = pd.NA
    return df


def set_zero_commute_time_as_zero_for_employed(
    df: pd.DataFrame,
) -> pd.DataFrame:
    df.loc[
        (df["employment"] == "employed") & (df["time_to_work"].isna()),
        "time_to_work",
    ] = 0
    return df


def df_drop_duplicates_merged_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function for removing duplicates after the data has been merged
    Only affects employed population. The reason for this is the 
    amount of missing data in the non-employed population in combination
    with the lack of a surrogate primary key field. 
    """
    logger.info("Removing Duplicates")
    length_before = len(df)
    df_filtered = df[(df["employment"] == "employed")]
    df_no_dupes = df_filtered.drop_duplicates()
    df = pd.concat([df_no_dupes, df[(df["employment"] != "employed")]])
    logger.info(
        f"{length_before - len(df)} rows removed, {len(df)} rows remaining"
    )

    return df
