from dataclasses import dataclass
from jarvis.common.TelegramBot import TelegramBot
from jarvis.common.TelegramChat import TelegramChat
from jarvis.common.TelegramMessage import TelegramMessage
from jarvis.common.FunctionType import FunctionType
from jarvis.common.functions.Function import Function

from jarvis.common.functions.FunctionCiao import FunctionCiao
from jarvis.common.functions.FunctionCallback import FunctionCallback
from jarvis.common.functions.FunctionProcess import FunctionProcess


@dataclass
class FunctionFactory:
    @staticmethod
    def get_function(function_type: FunctionType, bot: TelegramBot, chat: TelegramChat, message: TelegramMessage, function_id: int, is_new: bool) -> Function:
        match function_type:
            case FunctionType.CIAO:
                return FunctionCiao(bot=bot, chat=chat, message=message, function_type=FunctionType.CIAO, function_id=function_id, is_new=is_new)
            case FunctionType.CALLBACK:
                return FunctionCallback(bot=bot, chat=chat, message=message, function_type=FunctionType.CALLBACK, function_id=function_id, is_new=is_new)
            case FunctionType.PROCESS:
                return FunctionProcess(bot=bot, chat=chat, message=message, function_type=FunctionType.CALLBACK, function_id=function_id, is_new=is_new)
            case _:
                raise ValueError("Invalid function type")

