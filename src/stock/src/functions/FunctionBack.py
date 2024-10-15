from dataclasses import dataclass
from stock.src.functions.StockFunction import StockFunction


@dataclass
class FunctionBack(StockFunction):
    name: str = 'back'

    async def state_1(self):
        await self.send_message(chat_id=self.chat.chat_id, text='Main commands', default_keyboard=True)
        self.close_function()
