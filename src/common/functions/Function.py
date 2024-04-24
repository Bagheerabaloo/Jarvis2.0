from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from src.common.telegram.TelegramBot import TelegramBot
from src.common.telegram.TelegramChat import TelegramChat
from src.common.telegram.TelegramMessage import TelegramMessage
from src.common.telegram.TelegramFunction import TelegramFunction


@dataclass
class Function(ABC):
    function_id: int
    bot: TelegramBot
    chat: TelegramChat
    message: TelegramMessage
    # function_type: FunctionType
    is_new: bool = True
    telegram_function: TelegramFunction = field(init=False)

    def __post_init__(self):
        if self.is_new:
            self.telegram_function = self.chat.new_function(telegram_message=self.message, function_name=self.name)
        else:
            self.telegram_function = self.chat.get_function_by_message_id(message_id=self.function_id)

        self.evaluate()
        if self.is_new:
            self.chat.append_function(telegram_function=self.telegram_function)

    @property
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def evaluate(self):
        pass

    def send_message(self, chat_id: int, text: str, inline_keyboard: list = None):
        if inline_keyboard:
            response = self.bot.send_message(chat_id=chat_id, text=text, inline_keyboard=inline_keyboard)
            self.telegram_function.is_open_for_message = False
            self.telegram_function.has_inline_keyboard = True
            self.telegram_function.callback_message_id = response['message_id']
            return

        self.bot.send_message(chat_id=chat_id, text=text)
        return
        # def send_message(self, user_x, text, remove_keyboard=False, keyboard=None, force_inline=False, silent=True, force_sound=False,
        #                  pending=False, append_done=False, end_keyboard=None, bypass_inline=False,
        #                  accept_messages=True, accept_commands=True, parse_mode=None):

        # user_x.callback_id = None
        # user_x.accept_commands = accept_commands
        #
        # if user_x.is_callback and not bypass_inline:
        #     if keyboard == user_x.last_inline_keyboard and text == user_x.last_inline_text:
        #         return False
        #     self.telegram.edit_message(text=text, chat_id=user_x.callback_chat_id, message_id=user_x.callback_message_id, reply_markup=keyboard, pending=pending, parse_mode=parse_mode)
        #     user_x.is_callback = True
        #     user_x.last_inline_text = text
        #     user_x.last_inline_keyboard = keyboard
        #     return True
        #
        # elif keyboard and ((user_x.inline_mode and not bypass_inline) or force_inline) and not pending:
        #     response = self.telegram.send_message(chat_id=user_x.last_chat_id, text=text, inline_keyboard=keyboard, silent=silent, pending=pending, parse_mode=parse_mode)
        #     user_x.callback_message_id = response['message_id']
        #     user_x.callback_chat_id = response['chat_id']
        #     user_x.is_callback = True
        #     user_x.last_inline_text = text
        #     user_x.last_inline_keyboard = keyboard
        #     user_x.accept_messages = accept_messages
        #     return True
        #
        # if end_keyboard:
        #     keyboard = end_keyboard
        # elif append_done and keyboard:
        #     keyboard.append(['/Done'])
        # elif append_done:
        #     keyboard = [['/Back']]
        #
        # # if reply_keyboard != self.current_keyboard and self.is_main_keyboard(reply_keyboard):
        # #     self.current_keyboard = reply_keyboard.copy()
        #
        # silent = (silent or user_x.silent) and not force_sound
        # success = self.telegram.send_message(chat_id=user_x.last_chat_id, text=text, reply_keyboard=keyboard, remove_keyboard=remove_keyboard, silent=silent, pending=pending, parse_mode=parse_mode)
        # user_x.is_callback = False
        # return True if success else False

    def edit_message(self, chat_id: int, text: str, inline_keyboard: list = None):
        if inline_keyboard:
            response = self.bot.edit_message(message_id=self.telegram_function.callback_message_id, chat_id=chat_id, text=text, reply_markup=inline_keyboard)
            self.telegram_function.is_open_for_message = False
            self.telegram_function.has_inline_keyboard = True
            self.telegram_function.callback_message_id = response['message_id']
            return

        self.bot.send_message(chat_id=chat_id, text=text)
        return

    def close_function(self):
        if self.is_new:
            self.is_new = False
        else:
            self.chat.delete_function_by_message_id(message_id=self.function_id)


