from dataclasses import dataclass, field
from common import Function


@dataclass
class FunctionHelp(Function):
    name: str = 'help'

    async def state_1(self):
        text = "Here's a list of the main commands:\n/quote\n/showQuotes\n/back"
        await self.send_message(chat_id=self.chat.chat_id, text=text, default_keyboard=True)
        self.close_function()
