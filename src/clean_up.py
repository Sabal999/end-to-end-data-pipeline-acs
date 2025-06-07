import logging
from pathlib import Path
import logging
from logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__ + ".py")


def delete_all_files_in_folder(folder_path: Path):
    logger.info(f"Cleaning folder: {folder_path}")
    file_count = 0
    for file_path in folder_path.glob("*"):
        if file_path.is_file():
            file_path.unlink()
            file_count += 1
    logger.info(f"Deleted {file_count} files from {folder_path}")


def initial_cleanup(*paths: Path):

    for path in paths:
        delete_all_files_in_folder(path)
