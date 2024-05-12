from dataclasses import dataclass, field
from copy import deepcopy

from src.common.functions.FunctionType import FunctionType


@dataclass
class TelegramFunction:
    id: int                         # telegram message id
    name: str                       # telegram command (text)
    # function_type: FunctionType
    timestamp: int                  # telegram date
    update_id: int
    last_message_id: int
    # chat: dict              # telegram chat
    # original_caller: dict   # telegram from
    # current_function: str
    previous_state: int = 0
    state: int = 1
    is_open_for_message: bool = True
    has_inline_keyboard: bool = False
    callback_message_id: int = None
    # callback_id: int = None
    # callback_photo: bool = False

    settings: dict = field(default_factory=lambda: {})

    # last_caller: dict = None
    # state_function: int = 0
    # previous_state_function: int = 0
    # state: bool = True  # if the function is still valid
    # current_message: str = None
    # reserved: bool = False
    # auth_function: bool = False
    # variables: dict = None
    # input_variables: dict = None
    # output_variables: dict = None
    #

    # # callback_chat_id = None
    # last_inline_text: str = ''
    # last_inline_keyboard: list = None

    # def __post_init__(self):
    #     self.nested_function: TelegramFunction
    #     self.nested_function = None
    #     self.last_caller = self.original_caller

    def is_back(self, depth: int = 1) -> bool:
        return True if self.state == self.previous_state - depth else False

    def is_next(self, depth: int = 1) -> bool:
        return True if self.state == self.previous_state + depth else False

    def next(self, steps: int = 1):
        self.previous_state = self.state
        self.state += steps

    def back(self, steps: int = 1):
        self.previous_state = self.state
        self.state -= steps

    def same(self):
        self.previous_state = self.state

    # ___ changing name/state of function
    # def next_function(self, name=None, nxt=0, prev=0, reserved=None, last_message=None):
    #     self.current_function = name if name else self.current_function
    #     self.state_function = nxt
    #     self.previous_state_function = prev
    #     self.reserved = reserved if reserved else self.reserved
    #     self.current_message = last_message if last_message else None
    #     return self

    # def next(self, name=None, reserved=None, steps=1):
    #     return self.next_function(name=self.current_function,
    #                               nxt=name if name else self.state_function + steps,
    #                               prev=self.state_function,
    #                               reserved=reserved if reserved else self.reserved)

    # def back(self, name=None, reserved=None, steps=1):
    #     return self.next_function(name=self.current_function,
    #                               nxt=name if name else self.state_function - steps,
    #                               prev=self.state_function,
    #                               reserved=reserved if reserved else self.reserved)

    # def same(self, name=None, reserved=None, kwargs=None):
    #     return self.next_function(name=self.current_function,
    #                               nxt=name if name else self.state_function,
    #                               prev=self.state_function,
    #                               reserved=reserved if reserved else self.reserved)

    # ___ nesting functions ___
    def nest_function(self, name=None, nxt=None, prev=None, reserved=None):
        self.name = name if name else self.name
        self.state_function = nxt if nxt is not None else self.state_function + 1
        self.previous_state_function = prev if prev is not None else self.state_function
        self.reserved = reserved if reserved else self.reserved

        self.nested_function = deepcopy(self)
        self.output_variables = {}

    def nest_function_next(self):
        self.nest_function(nxt=self.state_function + 1, prev=self.state_function)

    def nest_function_back(self):
        self.nest_function(nxt=self.state_function - 1, prev=self.state_function)

    def back_to_master(self):
        if not self.nested_function:
            self.state = False
            return self

        self.input_variables = {}
        for key, value in self.nested_function.__dict__.items():
            setattr(self, key, value)
        return self

    def system_auto(self):
        return {"chat": self.chat['id'],
                "from_id": self.last_caller['id'],
                "from_name": self.last_caller['name'],
                "from_username": self.last_caller['username'],
                "text": 'system_auto'}

    def check_auth(self):
        return self.auth_function

    def is_same(self):
        if type(self.state_function) not in [int, float]:
            return False

        return True if self.state_function == self.previous_state_function else False

    def get(self, key):
        return getattr(self, key)


if __name__ == '__main__':
    response = {'update_id': 879617906,
                'message': {'message_id': 15,
                            'date': 1679074958,
                            'chat': {'id': 19838246,
                                     'type': 'private',
                                     'username': 'vales2',
                                     'first_name': 'Vale'},
                            'text': '/test',
                            'entities': [{'type': 'bot_command', 'offset': 0, 'length': 5}],
                            'caption_entities': [],
                            'photo': [],
                            'new_chat_members': [],
                            'new_chat_photo': [],
                            'delete_chat_photo': False,
                            'group_chat_created': False,
                            'supergroup_chat_created': False,
                            'channel_chat_created': False,
                            'from': {'id': 19838246, 'first_name': 'Vale', 'is_bot': False, 'username': 'vales2', 'language_code': 'it'}}}
    function = TelegramFunction(id=response['message']['message_id'], name=response['message']['text'].strip('/'), chat=response['message']['chat'],
                                timestamp=response['message']['date'], update_id=response['update_id'], current_function='test_app',
                                original_caller=response['message']['from'])
    function.next()
    function.nest_function_next()
    function.next_function(name='test_app_internal')
    function.back_to_master()
    function.back_to_master()
    print('end')


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