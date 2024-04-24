from dataclasses import dataclass, field
from src.common.functions.Function import Function


@dataclass
class FunctionCallback(Function):
    name: str = 'callback'

    def evaluate(self):
        match self.telegram_function.state:
            case 1:
                return self.state_1()
            case 2:
                return self.state_2()
            case 3:
                return self.state_3()
            case 4:
                return self.state_4()
            case _:
                raise ValueError("Invalid function type")

    def state_1(self):
        chat_id = self.chat.chat_id
        text = '1'
        self.send_message(chat_id=chat_id, text=text, inline_keyboard=["<", ">"])
        self.telegram_function.state = 2

    def state_2(self):
        chat_id = self.chat.chat_id
        text = ''
        if self.message.data == '<':
            text = '3'
            self.telegram_function.state = 3
        elif self.message.data == '>':
            text = '2'
            self.telegram_function.state = 4

        self.edit_message(chat_id=chat_id, text=text, inline_keyboard=["<", ">"])

    def state_3(self):
        chat_id = self.chat.chat_id
        text = ''
        if self.message.data == '<':
            text = '2'
            self.telegram_function.state = 4
        elif self.message.data == '>':
            text = '1'
            self.telegram_function.state = 2

        self.edit_message(chat_id=chat_id, text=text, inline_keyboard=["<", ">"])

    def state_4(self):
        chat_id = self.chat.chat_id
        text = ''
        if self.message.data == '<':
            text = '1'
            self.telegram_function.state = 2
        elif self.message.data == '>':
            text = '3'
            self.telegram_function.state = 3

        self.edit_message(chat_id=chat_id, text=text, inline_keyboard=["<", ">"])