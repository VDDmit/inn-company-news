import logging
import os


def get_logger(log_file_name="app.log", log_dir="logs", level=logging.INFO):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(log_file_name)
    logger.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fh = logging.FileHandler(os.path.join(log_dir, log_file_name), encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
