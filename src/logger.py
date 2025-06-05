import logging
import os


def configure_logging():
    log_folder = "output/logs"
    os.makedirs(log_folder, exist_ok=True)

    logging.basicConfig(
        filename=f"{log_folder}/pipeline.log",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )
