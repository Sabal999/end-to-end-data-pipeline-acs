"""
Simple logging configuration module.

Sets up a logger to write to output/logs/pipeline.log.

Log level: INFO
Log format: timestamp, logger name, message.
"""

import logging
import os


def configure_logging():
    log_folder = "output/logs"
    os.makedirs(log_folder, exist_ok=True)

    logging.basicConfig(
        filename=f"{log_folder}/pipeline.log",
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
