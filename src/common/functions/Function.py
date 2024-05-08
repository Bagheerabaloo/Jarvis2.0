import math
from src.common.tools.library import to_int
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from src.common.telegram.TelegramBot import TelegramBot
from src.common.telegram.TelegramChat import TelegramChat
from src.common.telegram.TelegramMessage import TelegramMessage
from src.common.telegram.TelegramUser import TelegramUser
from src.common.telegram.TelegramFunction import TelegramFunction
from src.common.postgre.PostgreManager import PostgreManager


@dataclass
class Function(ABC):
    function_id: int
    bot: TelegramBot
    chat: TelegramChat
    message: TelegramMessage
    # function_type: FunctionType
    is_new: bool = field(default=True)
    telegram_user: TelegramUser = field(default=None)
    postgre_manager: PostgreManager = field(default=None)
    telegram_function: TelegramFunction = field(init=False)
    need_to_update_users: bool = False

    @property
    def default_keyboard(self):
        return [['ciao'], ['callback']]

    @property
    def app_user(self):
        return self.telegram_user

    def initialize(self):
        if self.is_new:
            self.telegram_function = self.chat.new_function(telegram_message=self.message, function_name=self.name)
        else:
            self.telegram_function = self.chat.get_function_by_message_id(message_id=self.function_id)

    async def evaluate(self):
        match self.telegram_function.state:
            case 1:
                return await self.state_1()
            case 2:
                return await self.state_2()
            case 3:
                return await self.state_3()
            case 4:
                return await self.state_4()
            case 5:
                return await self.state_5()
            case 6:
                return await self.state_6()
            case 7:
                return await self.state_7()
            case 8:
                return await self.state_8()
            case 9:
                return await self.state_9()
            case 10:
                return await self.state_10()
            case _:
                raise ValueError("Invalid state")

    def post_evaluate(self):
        if self.is_new:
            self.chat.append_function(telegram_function=self.telegram_function)

    async def execute(self):
        self.initialize()
        await self.evaluate()
        self.post_evaluate()

    async def send_message(self, chat_id: int,
                           text: str,
                           inline_keyboard: list = None,
                           keyboard: list = None,
                           parse_mode: str = None,
                           open_for_messages: bool = False,
                           default_keyboard: bool = False):
        if inline_keyboard:
            response = await self.bot.send_message(chat_id=chat_id, text=text, inline_keyboard=inline_keyboard, parse_mode=parse_mode)
            self.telegram_function.is_open_for_message = False or open_for_messages
            self.telegram_function.has_inline_keyboard = True
            self.telegram_function.callback_message_id = response['message_id']
            return
        elif keyboard or default_keyboard:
            keyboard = keyboard if keyboard else self.default_keyboard
            response = await self.bot.send_message(chat_id=chat_id, text=text, reply_keyboard=keyboard, parse_mode=parse_mode)
            self.telegram_function.is_open_for_message = True
            self.telegram_function.has_inline_keyboard = False
            self.telegram_function.callback_message_id = response['message_id']
            return

        await self.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
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

    async def edit_message(self, chat_id: int, text: str, inline_keyboard: list = None, parse_mode=None):
        if inline_keyboard:
            response = await self.bot.edit_message(message_id=self.telegram_function.callback_message_id, chat_id=chat_id, text=text, reply_markup=inline_keyboard, parse_mode=parse_mode)
            self.telegram_function.is_open_for_message = False
            self.telegram_function.has_inline_keyboard = True
            self.telegram_function.callback_message_id = response['message_id']
            return

        await self.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
        return

    def send_callback(self, chat: TelegramChat, message: TelegramMessage, text: str):
        if message.callback_id:
            # callback_id = user_x.callback_id
            # user_x.callback_id = None
            return self.bot.send_callback(callback_id=message.callback_id, text=text)
        elif not message.callback_id:
            # return self.send_message(chat_id=chat.chat_id, text=text, pending=True)
            return self.send_message(chat_id=chat.chat_id, text=text)

        return False

    def close_function(self):
        if self.is_new:
            self.is_new = False
        else:
            self.chat.delete_function_by_message_id(message_id=self.function_id)

    @property
    @abstractmethod
    def name(self):
        pass

    @staticmethod
    def square_keyboard(inputs):
        if not inputs:
            return inputs

        l = len(inputs)
        rows = to_int(math.floor(math.sqrt(l)))
        (quot, rem) = divmod(l, rows)
        qty = [quot] * rows
        pos = 0
        while rem > 0:
            qty[pos] += 1
            pos += 1
            rem -= 1
        count = 0
        keyboard = []
        for i in range(rows):
            vector = []
            for j in range(qty[i]):
                vector.append(str(inputs[count]))
                count += 1
            keyboard.append(vector)
        return keyboard

    @staticmethod
    def build_from(new_user: TelegramUser):
        return f"ID: _{new_user.telegram_id}_\nname: _{new_user.name}_\nusername: _{new_user.username}_"

    @staticmethod
    def build_navigation_keyboard(index, len_):
        keyboard = []
        if index > 0:
            keyboard.append('<<')
            keyboard.append('<')
        if index < len_ - 1:
            keyboard.append('>')
            keyboard.append('>>')
        return keyboard

    async def state_1(self):
        pass

    async def state_2(self):
        pass

    async def state_3(self):
        pass

    async def state_4(self):
        pass

    async def state_5(self):
        pass

    async def state_6(self):
        pass

    async def state_7(self):
        pass

    async def state_8(self):
        pass

    async def state_9(self):
        pass

    async def state_10(self):
        pass



