from dataclasses import dataclass, field

from common import Function


@dataclass
class FunctionCiao(Function):
    name: str = "ciao"

    async def evaluate(self):
        match self.telegram_function.state:
            case 1:
                return await self.state_1()
            case 2:
                return await self.state_2()
            case _:
                raise ValueError("Invalid function type")

    async def state_1(self):
        chat_id = self.chat.chat_id
        text = 'scrivi ciao'
        await self.send_message(chat_id=chat_id, text=text)
        self.telegram_function.state = 2

    async def state_2(self):
        if self.message.text == 'ciao':
            chat_id = self.chat.chat_id
            text = 'Bravo!'
            await self.send_message(chat_id=chat_id, text=text)
            self.close_function()
        else:
            chat_id = self.chat.chat_id
            text = 'Try again'
            await self.send_message(chat_id=chat_id, text=text)

