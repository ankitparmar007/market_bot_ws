from enum import Enum


class TGCommands(Enum):
    # Start Websocket
    START = "/start"
    # Stop Websocket
    STOP = "/stop"
    # Status of Websocket
    STATUS = "/status"
    # Current Docs length available in Buffer
    DOCS = "/docs"
    # Flush Docs Buffer to DB
    FLUSH_DOCS = "/flush_docs"
    # Start update_r_factor_loop
    START_UPDATE_R_FACTOR = "/start_update_r_factor"
    # Stop update_r_factor_loop
    STOP_UPDATE_R_FACTOR = "/stop_update_r_factor"
