from enum import Enum


class TelegramMessageType(Enum):
    MESSAGE = "message"
    COMMAND = "command"
    CALLBACK = "callback"

