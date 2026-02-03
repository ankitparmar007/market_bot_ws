from enum import Enum


class TGCommands(Enum):
    # Get available commands
    START = "/start"
    # Start Websocket
    START_WS = "/start_ws"
    # Stop Websocket
    STOP_WS = "/stop_ws"
    # Status of Websocket
    STATUS = "/status"
    # Start update_r_factor_loop
    START_UPDATE_R_FACTOR = "/start_update_r_factor"
    # Stop update_r_factor_loop
    STOP_UPDATE_R_FACTOR = "/stop_update_r_factor"
    # Start update_oi_loop
    START_UPDATE_OI = "/start_update_oi"
    # Stop update_oi_loop
    STOP_UPDATE_OI = "/stop_update_oi"
    # Refresh Token
    REFRESH_TOKEN = "/refresh_token"

