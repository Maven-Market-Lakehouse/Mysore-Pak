import logging

def get_logger():
    logger = logging.getLogger("maven_logger")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        ch = logging.StreamHandler()
        logger.addHandler(ch)

    return logger