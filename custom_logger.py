import logging
import sys

def setup_logger(name, log_file='crawler.log'):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG) 
    
    formatter = logging.Formatter('%(asctime)s | %(levelname)-7s | %(funcName)-20s | %(message)s')
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
    return logger

log = setup_logger(__name__)