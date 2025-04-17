import logging

from prefect.context import FlowRunContext, TaskRunContext
from prefect import get_run_logger

def get_logger():
    # Check if running within a Prefect runtime context
    if FlowRunContext.get() or TaskRunContext.get():
        return get_run_logger()
    else:
        # Return a standard Python logger when not running in Prefect
        logger = logging.getLogger('databot')
        if not logger.hasHandlers():
            # Only add handler if no handlers exist
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        logger.setLevel(logging.INFO)
        return logger
