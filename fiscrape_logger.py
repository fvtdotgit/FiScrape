import logging

# Logger shares the same name with the file (fiscrape_logger)
logger = logging.getLogger(__name__)

# Set up handler
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler('fiscrape_logging.log')

# Setting up the level
stream_handler.setLevel(logging.WARNING)
file_handler.setLevel(logging.INFO)

# Formatting the log outputs
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Adding the handlers to the logger
logger.addHandler(stream_handler)
logger.addHandler(file_handler)
