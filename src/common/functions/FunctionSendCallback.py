from dataclasses import dataclass, field
from common.functions.Function import Function


@dataclass
class FunctionSendCallback(Function):
    name: str = 'send_callback'

    async def state_1(self):
        await self.send_callback(chat=self.chat, message=self.message, text='Function has expired')
        self.close_function()
