from dataclasses import dataclass, field
from src.common.telegram.TelegramBot import TelegramBot
from src.common.telegram.TelegramChat import TelegramChat
from src.common.functions.Function import Function
from src.common.telegram.TelegramFunction import TelegramFunction


@dataclass
class FunctionCiao(Function):
    name: str = "ciao"

    def evaluate(self):
        match self.telegram_function.state:
            case 1:
                return self.state_1()
            case 2:
                return self.state_2()
            case _:
                raise ValueError("Invalid function type")

    def state_1(self):
        chat_id = self.chat.chat_id
        text = 'scrivi ciao'
        self.send_message(chat_id=chat_id, text=text)
        self.telegram_function.state = 2

    def state_2(self):
        if self.message.text == 'ciao':
            chat_id = self.chat.chat_id
            text = 'Bravo!'
            self.send_message(chat_id=chat_id, text=text)
            self.close_function()
        else:
            chat_id = self.chat.chat_id
            text = 'Try again'
            self.send_message(chat_id=chat_id, text=text)

