from dataclasses import dataclass, field, asdict
from src.common.telegram_manager.TelegramMessageType import TelegramMessageType


# TODO: move to sqlAlchemy
@dataclass
class TelegramMessage:
    message_type: TelegramMessageType
    chat_id: int
    message_id: int
    date: int
    update_id: int
    from_id: int
    from_name: str
    from_username: str
    chat_last_name: str

    text: str = None
    callback_id: int = None
    data: str = None

    def to_dict(self):
        result = asdict(self)
        for key, value in result.items():
            if hasattr(value, 'to_dict'):
                result[key] = value.to_dict()
        return result

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def get_from(self):
        return {k: v for k, v in self.__dict__.items() if k.startswith('from')}

    def last_message(self):
        return self.text if self.text else self.data

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
