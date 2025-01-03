from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from threading import Thread, current_thread
from time import sleep
from typing import Optional, List, Type, TYPE_CHECKING

from src.common.functions.Function import Function
from src.common.functions.FunctionSendCallback import FunctionSendCallback

from src.common.telegram_manager.telegram_manager import TelegramManager, LOGGER
from src.common.telegram_manager.TelegramMessage import TelegramMessage
from src.common.telegram_manager.TelegramMessageType import TelegramMessageType


@dataclass
class TelegramOperations:
    manager: TelegramManager
    main_thread: Optional[Thread] = field(default=None, init=False)
    loop: Optional[asyncio.AbstractEventLoop] = field(default=None, init=False)
    stopping: bool = field(default=False, init=False)

    def __post_init__(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def start_main_thread(self):
        self.manager.run = True
        self.main_thread = Thread(target=self.__run_main_thread, name=f'{self.manager.name}MainThread')
        self.main_thread.start()

    def stop_main_thread(self):
        self.manager.run = False
        if self.main_thread and self.main_thread != current_thread():
            self.main_thread.join()
        else:
            if self.loop and self.loop.is_running():
                LOGGER.debug("Shutting down main event loop.")
                self.stopping = True
                self.loop.call_soon_threadsafe(self._stop_asyncio_loop)
            else:
                if self.loop:
                    self.loop.close()
                    LOGGER.debug("Main event loop closed.")

    def _stop_asyncio_loop(self):
        asyncio.create_task(self._shutdown_loop(self.loop))

    @staticmethod
    async def _shutdown_loop(loop):
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]
        LOGGER.debug(f"Shutting down {len(tasks)} tasks.")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()

    def __run_main_thread(self):
        asyncio.set_event_loop(self.loop)
        task = self.loop.create_task(self.__main_thread(), name='MainThreadTask')
        try:
            self.loop.run_until_complete(task)
        except (SystemExit, KeyboardInterrupt):
            pass
        finally:
            if self.stopping:
                self.loop.run_until_complete(self._shutdown_loop(self.loop))
                self.loop.close()
                LOGGER.info("Main event loop closed after run_until_complete.")

    async def __main_thread(self):
        """this function is the main thread that handles the messages received from the telegram bot"""
        while self.manager.run:
            if not self.manager.update_stream.empty():
                telegram_message = self.manager.update_stream.get_nowait()
                print(telegram_message)  # TODO: delete this line
                if telegram_message.message_type == TelegramMessageType.COMMAND and telegram_message.text.upper() == 'END':
                    self.manager.run = False
                    break
                else:
                    await self.__handle_event(telegram_message=telegram_message)
            await asyncio.sleep(0.05)

    async def __handle_event(self, telegram_message: TelegramMessage):
        """this function is called when a new message is received from the telegram bot"""
        # __ identify the user who sent the update __
        user_x = next((x for x in self.manager.app_users if x.telegram_id == telegram_message.from_id), None)

        # __ handle command start from new users or existing users in other apps __
        if telegram_message.text == 'start':
            return await self.manager.handle_command_start(message=telegram_message, user_x=user_x)

        # __ handle commands different from start from new users (do nothing) __
        if not user_x:
            return None

        # __ get chat __
        telegram_chat = next((x for x in self.manager.chats if x.chat_id == telegram_message.chat_id), None)
        if not telegram_chat:
            LOGGER.error('CHAT NOT FOUND')
            return

        telegram_chat.new_message(telegram_message=telegram_message)
        # TODO: handle functions that are restricted -> they can't be called from here
        try:
            if telegram_message.message_type == TelegramMessageType.COMMAND:
                command = telegram_message.text.strip('/')
                await self.manager.function_handler.execute_command(user_x=user_x, command=command, message=telegram_message, chat=telegram_chat)
            elif telegram_message.message_type == TelegramMessageType.MESSAGE:
                await self.__handle_message(user_x=user_x, message=telegram_message, chat=telegram_chat)
            elif telegram_message.message_type == TelegramMessageType.CALLBACK:
                await self.__handle_callback(user_x=user_x, message=telegram_message, chat=telegram_chat)
        except Exception as e:
            LOGGER.error(f"Exception handling event: {e}")

    async def __handle_message(self, user_x, message, chat):
        telegram_function = chat.get_function_open_for_message()
        if telegram_function:
            functions: List[Type[Function]] = self.manager.function_handler.get_function_by_name(name=telegram_function.name, user_x=user_x)
            if functions:
                return await self.manager.function_handler.run_existing_function(
                    function=functions[0],
                    function_id=telegram_function.id,
                    user_x=user_x,
                    chat=chat,
                    message=message)

        functions = self.manager.function_handler.get_functions_by_alias(alias=message.text.strip('/'), user_x=user_x)
        if functions:
            return await self.manager.function_handler.run_new_function(function=functions[0],
                                                                        user_x=user_x,
                                                                        chat=chat,
                                                                        message=message)
        await self.manager.call_message(user_x=user_x, message=message, chat=chat, txt='')

    async def __handle_callback(self, user_x, message, chat):
        telegram_function = chat.get_function_by_callback_message_id(callback_message_id=message.message_id)
        if telegram_function:
            functions: List[Type[Function]] = self.manager.function_handler.get_function_by_name(name=telegram_function.name, user_x=user_x)
            if functions:
                return await self.manager.function_handler.run_existing_function(
                    function=functions[0],
                    function_id=telegram_function.id,
                    user_x=user_x,
                    chat=chat,
                    message=message)
        else:
            function = FunctionSendCallback  # TODO: handle with functions run_existing_function and run_new_function
            fun = self.manager.function_handler.instantiate_function(function=function,
                                                                     chat=chat,
                                                                     message=message,
                                                                     is_new=True,
                                                                     function_id=self.manager.get_next_available_function_id(),
                                                                     user_x=user_x)
            await self.manager.function_handler.execute_function(function=fun, user_x=user_x)
