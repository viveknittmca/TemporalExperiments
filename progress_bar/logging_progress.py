import logging
import time
from tqdm import tqdm

# Set up your logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tqdm_logger")

class TqdmLoggerStream:
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level
        self.buffer = ""

    def write(self, message):
        # Remove carriage returns/newlines and write non-empty messages
        message = message.strip()
        if message:
            self.logger.log(self.level, message)

    def flush(self):
        # No action needed for flush in this simple implementation
        pass

# Create an instance of the logger stream
tqdm_log_stream = TqdmLoggerStream(logger, logging.INFO)

# Pass the custom logger stream to tqdm via the file parameter
for i in tqdm(range(10), desc="Processing", file=tqdm_log_stream):
    time.sleep(0.5)
    logger.info("Processing item %d", i)


# Another approach
# import logging
# import time
# from tqdm import tqdm
# from tqdm.contrib.logging import logging_redirect_tqdm
#
# # Configure your logger
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
#
# # Use logging_redirect_tqdm to send tqdm output to logging
# with logging_redirect_tqdm():
#     for i in tqdm(range(10), desc="Processing"):
#         time.sleep(0.5)
#         logger.info("Processing item %d", i)
