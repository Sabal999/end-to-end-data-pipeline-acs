"""
Simple logging configuration module.

Log level: INFO
Log format: timestamp, logger name, message.
"""

import logging


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
