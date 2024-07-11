from enum import Enum


class TelegramMessageType(Enum):
    MESSAGE = "message"
    COMMAND = "command"
    CALLBACK = "callback"

    def to_dict(self):
        return self.name

    @classmethod
    def from_dict(cls, data: str):
        return cls(data)


