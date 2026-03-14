import logging
import logging.config
from copy import deepcopy

from data_structuring.config import DEFAULT_LOGGING_CONFIG, RPC_LOGGING_FORMAT, RPC_LOGGING_FORMAT_TOKEN


# Function to add an RPC ID to the logging config
def set_logging_config(rpc_id: str) -> None:
    logging_config = deepcopy(DEFAULT_LOGGING_CONFIG)
    logging_format = RPC_LOGGING_FORMAT.format(**{RPC_LOGGING_FORMAT_TOKEN: f"[RPC-{rpc_id}]"})
    logging_config["formatters"]["standard"]["format"] = logging_format
    logging.config.dictConfig(logging_config)


# Function to remove the RPC ID from the logging config
def unset_logging_config() -> None:
    logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
