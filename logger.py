import logging

def setup_logger(name: str = None, level: int = logging.INFO) -> logging.Logger:
    """
    Create and return a logger with a standard format
    
    Args:
        name: optional logger name. If it's None, root logger is used.
        level: (INFO, WARNING, ERROR, CRITICAL)
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # prevent adding multiple handlers if the logger already has one
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger
