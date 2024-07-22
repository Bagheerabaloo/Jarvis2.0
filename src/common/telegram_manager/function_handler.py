from __future__ import annotations

from dataclasses import dataclass
from typing import Type, Optional, List, TYPE_CHECKING

from common.telegram_manager.telegram_manager import TelegramManager
from common.telegram_manager.TelegramMessage import TelegramMessage
from common.telegram_manager.TelegramUser import TelegramUser
from common.telegram_manager.TelegramChat import TelegramChat
from common.functions.Function import Function


@dataclass
class FunctionHandler:
    manager: TelegramManager

    """This class is used to handle the functions associated to the commands. It is used to instantiate the functions and to execute them."""
    async def get_function_by_alias(self, alias: str, chat_id: int, user_x: TelegramUser) -> Optional[Type[Function]]:
        functions = self.get_functions_by_alias(alias=alias, user_x=user_x)
        if len(functions) == 0:
            await self.manager.telegram_bot.send_message(chat_id=chat_id, text="Function not implemented or forbidden")
            return None
        elif len(functions) > 1:
            await self.manager.telegram_bot.send_message(chat_id=chat_id, text="Warning: more than one function associated to this alias")
            return None
        return functions[0]

    def get_functions_by_alias(self, alias: str, user_x: TelegramUser) -> List[Type[Function]]:
        return [x.function for x in self.manager.commands if alias.lower() in [y.lower() for y in x.alias] and (not x.admin or user_x.is_admin)]

    def get_not_admin_functions_by_alias(self, alias: str) -> List[Type[Function]]:
        # Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao, needs_postgre=True),
        return [x.function for x in self.manager.commands if alias.lower() in [y.lower() for y in x.alias] and not x.admin]

    def get_function_by_name(self, name: str, user_x: TelegramUser) -> List[Type[Function]]:
        # Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao),
        # TODO: add restricted constraint
        return [x.function for x in self.manager.commands if name == x.function.name and (not x.admin or user_x.is_admin)]

    async def execute_command(self,
                              user_x: TelegramUser,
                              command: str,
                              message: TelegramMessage,
                              chat: TelegramChat,
                              initial_settings: dict = None,
                              initial_state: int = 1):  # TODO: add type of output
        function = await self.get_function_by_alias(alias=command, chat_id=message.chat_id, user_x=user_x)
        if not function:
            return False
        return await self.run_new_function(function=function,
                                           user_x=user_x,
                                           chat=chat,
                                           message=message,
                                           initial_settings=initial_settings,
                                           initial_state=initial_state)

    async def run_new_function(self,
                               function: Type[Function],
                               user_x: TelegramUser,
                               chat: TelegramChat,
                               message: TelegramMessage,
                               initial_settings: dict = None,
                               initial_state: int = 1):
        initialized_function = self.instantiate_function(function=function,
                                                         chat=chat,
                                                         message=message,
                                                         is_new=True,
                                                         function_id=message.message_id,
                                                         user_x=user_x)
        await self.execute_function(function=initialized_function,
                                    user_x=user_x,
                                    initial_settings=initial_settings,
                                    initial_state=initial_state)
        return initialized_function

    async def run_existing_function(self, function: Type[Function], function_id: int, user_x: TelegramUser, chat: TelegramChat, message: TelegramMessage):
        initialized_function = self.instantiate_function(function=function,
                                                         chat=chat,
                                                         message=message,
                                                         is_new=False,
                                                         function_id=function_id,
                                                         user_x=user_x)
        await self.execute_function(function=initialized_function, user_x=user_x)
        return initialized_function

    def instantiate_function(self, function,
                             chat: TelegramChat,
                             message: TelegramMessage,
                             is_new: bool,
                             function_id: int,
                             user_x: TelegramUser) -> Function:
        return function(bot=self.manager.telegram_bot,
                        chat=chat,
                        message=message,
                        function_id=function_id,
                        is_new=is_new,
                        postgre_manager=self.manager.postgre_manager,
                        user=user_x)

    async def execute_function(self, function: Function, user_x: TelegramUser, initial_settings: dict = None, initial_state: int = 1):
        # __ execute the function normally __
        await function.execute(initial_settings=initial_settings, initial_state=initial_state)

        # __ checks if users must be refreshed __
        if function.need_to_update_users:
            self.manager.update_users()

        # __ checks if a new function should be opened __
        if function.need_to_instantiate_new_function:
            initial_state = function.telegram_function.settings.pop("initial_state", 1)
            new_function = function.telegram_function.settings.pop("new_function")
            await self.run_new_function(function=new_function,
                                        user_x=user_x,
                                        chat=function.chat,
                                        message=function.message,
                                        initial_settings=function.telegram_function.settings,
                                        initial_state=initial_state)


