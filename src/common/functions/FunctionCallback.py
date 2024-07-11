from dataclasses import dataclass, field
from common import Function


@dataclass
class FunctionCallback(Function):
    name: str = 'callback'

    async def state_1(self):
        chat_id = self.chat.chat_id
        text = '1'
        await self.send_message(chat_id=chat_id, text=text, inline_keyboard=["<", ">"])
        self.telegram_function.state = 2

    async def state_2(self):
        chat_id = self.chat.chat_id
        text = ''
        if self.message.data == '<':
            text = '3'
            self.telegram_function.state = 3
        elif self.message.data == '>':
            text = '2'
            self.telegram_function.state = 4

        await self.edit_message(chat_id=chat_id, text=text, inline_keyboard=["<", ">"])

    async def state_3(self):
        chat_id = self.chat.chat_id
        text = ''
        if self.message.data == '<':
            text = '2'
            self.telegram_function.state = 4
        elif self.message.data == '>':
            text = '1'
            self.telegram_function.state = 2

        await self.edit_message(chat_id=chat_id, text=text, inline_keyboard=["<", ">"])

    async def state_4(self):
        chat_id = self.chat.chat_id
        text = ''
        if self.message.data == '<':
            text = '1'
            self.telegram_function.state = 2
        elif self.message.data == '>':
            text = '3'
            self.telegram_function.state = 3

        await self.edit_message(chat_id=chat_id, text=text, inline_keyboard=["<", ">"])