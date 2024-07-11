from dataclasses import dataclass, field
from typing import List
from common import TelegramFunction
from common import TelegramMessage


@dataclass
class TelegramChat:
    chat_id: int
    type: str
    title: str = None
    username: str = None
    first_name: str = None
    last_name: str = None

    # __ flags and settings __
    inline_mode: bool = True
    silent: bool = False

    # __ chat history __
    messages: List[TelegramMessage] = field(default_factory=lambda: [])
    running_functions: List[TelegramFunction] = field(default_factory=lambda: [])

    def new_message(self, telegram_message: TelegramMessage):
        self.messages.append(telegram_message)

    # __ functions __
    def new_function(self, telegram_message: TelegramMessage, function_name: str):
        self.reset_open_functions()
        return TelegramFunction(id=telegram_message.message_id,
                                name=function_name,
                                # function_type=function_type,
                                timestamp=telegram_message.date,
                                update_id=telegram_message.update_id,
                                last_message_id=telegram_message.message_id
                                )

    def append_function(self, telegram_function: TelegramFunction):
        self.running_functions.append(telegram_function)

    def reset_open_functions(self):
        for function in self.running_functions:
            function.is_open_for_message = False

    def get_function_by_message_id(self, message_id: int) -> TelegramFunction:
        functions = [x for x in self.running_functions if x.id == message_id]
        return functions[0] if len(functions) > 0 else None

    def get_function_by_callback_message_id(self, callback_message_id: int) -> TelegramFunction:
        functions = [x for x in self.running_functions if x.callback_message_id == callback_message_id]
        return functions[0] if len(functions) > 0 else None

    def get_function_open_for_message(self) -> TelegramFunction:
        functions = sorted([x for x in self.running_functions if x.is_open_for_message], key=lambda x: x.update_id, reverse=True)
        return functions[0] if len(functions) > 0 else None

    def delete_function_by_message_id(self, message_id: int):
        self.running_functions = [x for x in self.running_functions if x.id != message_id]


if __name__ == '__main__':
    pass


""" New Chat
{'update_id': 879617908, 
 'message': {'message_id': 9265, 
             'date': 1679134499, 
             'chat': {'id': -1001664351197, 'type': 'supergroup', 'title': 'No Time For Canarie'}, 
             'entities': [], 'caption_entities': [], 'photo': [], 
             'new_chat_members': [{'id': 5712307917, 'first_name': 'No Time for Canarie', 'is_bot': True, 'username': 'no_time_for_canarie_bot'}], 
             'new_chat_photo': [], 
             'delete_chat_photo': False, 
             'group_chat_created': False, 
             'supergroup_chat_created': False, 
             'channel_chat_created': False, 
             'from': {'id': 19838246, 'first_name': 'Vale', 'is_bot': False, 'username': 'vales2', 'language_code': 'it'}}}
"""

"""
{'update_id': 879617909,
 'message': {'message_id': 18, 
             'date': 1679134658, 
             'chat': {'id': 21397600, 'type': 'private', 'username': 'ziolomu', 'first_name': 'Andrea', 'last_name': 'L'}, 
             'text': '/start', 'entities': [{'type': 'bot_command', 'offset': 0, 'length': 6}], 
             'caption_entities': [], 'photo': [], 'new_chat_members': [], 'new_chat_photo': [], 
             'delete_chat_photo': False, 'group_chat_created': False, 'supergroup_chat_created': False, 'channel_chat_created': False,
             'from': {'id': 21397600, 'first_name': 'Andrea', 'is_bot': False, 'last_name': 'L', 'username': 'ziolomu', 'language_code': 'it'}}}
             """

"""
{'update_id': 879617910, 
 'message': {'message_id': 19, 'date': 1679134685, 
             'chat': {'id': 60481756, 'type': 'private', 'username': 'Matte091', 'first_name': 'Matteo', 'last_name': 'Pacifico'}, 
             'text': '/start', 'entities': [{'type': 'bot_command', 'offset': 0, 'length': 6}], 
             'caption_entities': [], 'photo': [], 'new_chat_members': [], 'new_chat_photo': [], 
             'delete_chat_photo': False, 'group_chat_created': False, 'supergroup_chat_created': False, 
             'channel_chat_created': False, 
             'from': {'id': 60481756, 'first_name': 'Matteo', 'is_bot': False, 'last_name': 'Pacifico', 'username': 'Matte091', 'language_code': 'it'}}}
"""

"""{'update_id': 879617910, 
 'message': {'message_id': 19, 
             'date': 1679134685, 
             'chat': {'id': 60481756, 'type': 'private', 'username': 'Matte091', 'first_name': 'Matteo', 'last_name': 'Pacifico'}, 
             'text': '/start', 
             'entities': [{'type': 'bot_command', 'offset': 0, 'length': 6}], 
             'caption_entities': [], 
             'photo': [], 
             'new_chat_members': [], 
             'new_chat_photo': [], 
             'delete_chat_photo': False, 
             'group_chat_created': False, 
             'supergroup_chat_created': False, 
             'channel_chat_created': False, 
             'from': {'id': 60481756, 'first_name': 'Matteo', 'is_bot': False, 'last_name': 'Pacifico', 'username': 'Matte091', 'language_code': 'it'}}}"""

"""
{'update_id': 879617911, 
 'message': {'message_id': 9275, 
             'date': 1679135126, 
             'chat': {'id': -1001664351197, 'type': 'supergroup', 'title': 'No Time For Canarie'}, 
             'text': '/start', 
             'entities': [{'type': 'bot_command', 'offset': 0, 'length': 6}], 
             'caption_entities': [], 
             'photo': [], 
             'new_chat_members': [], 
             'new_chat_photo': [], 
             'delete_chat_photo': False, 
             'group_chat_created': False, 
             'supergroup_chat_created': False, 
             'channel_chat_created': False, 
             'from': {'id': 19838246, 'first_name': 'Vale', 'is_bot': False, 'username': 'vales2', 'language_code': 'it'}}}
"""
