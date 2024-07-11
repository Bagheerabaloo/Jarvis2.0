from dataclasses import dataclass, field
from src.common.tools.library import to_int

from src.common.functions.Function import Function


@dataclass
class FunctionTotalDBRows(Function):
    name: str = 'process'

    async def state_1(self):
        total_rows = self.postgre_manager.get_total_number_of_rows_for_all_tables()
        text = f"Total number of rows in DB: {total_rows}"
        await self.send_message(chat_id=self.chat.chat_id, text=text)
        self.close_function()
