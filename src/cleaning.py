import logging
from logger import configure_logging
import pandas as pd
import aiofiles
import os
from datetime import datetime
from io import StringIO
from pathlib import Path


configure_logging()
logger = logging.getLogger(__name__)


# Your existing cleaning function
def df_cleaner(df: pd.DataFrame) -> pd.DataFrame:
    # Implement your cleaning here
    df["income"] = 0
    return df  # placeholder


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
    name, ext = os.path.splitext(base_name)
    now = datetime.now()
    timestamp = (
        now.strftime("%Y_%m_%d_%H_%M_%S_")
        + f"{now.microsecond // 1000:03d}"
    )
    output_file = os.path.join(
        output_dir, f"{name}_cleaned_{timestamp}{ext}"
    )

    # Save cleaned DataFrame synchronously (pandas doesn't support async writes)
    clean_df.to_csv(output_file, index=False)
    logger.info(f"Processed and saved: {output_file}")
