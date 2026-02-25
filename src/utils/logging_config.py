import logging
import os
from logging.handlers import RotatingFileHandler


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(level=logging.DEBUG)

    if not logger.handlers:
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        ch = logging.StreamHandler()
        ch.setLevel(level=logging.INFO)

        fh = RotatingFileHandler(
            filename="logs/job.log", maxBytes=10000, backupCount=3, mode="a"
        )

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        ch.setFormatter(formatter)
        fh.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger
