from __future__ import annotations

import asyncio
from time import sleep
from threading import Thread
from typing import Optional, TYPE_CHECKING
from dataclasses import dataclass, field

from common.telegram_manager.telegram_manager import TelegramManager, LOGGER
from common.telegram_manager.TelegramMessage import TelegramMessage
from common.telegram_manager.TelegramMessageType import TelegramMessageType


@dataclass
class PollingHandler:
    manager: TelegramManager
    polling_thread: Optional[Thread] = field(default=None, init=False)
    loop: Optional[asyncio.AbstractEventLoop] = field(default=None, init=False)

    def start_polling_thread(self):
        self.polling_thread = Thread(target=self.__run_polling, name=f'{self.manager.name}PollingThread')
        self.polling_thread.start()

    def stop_polling_thread(self):
        self.manager.run = False
        if self.polling_thread:
            self.polling_thread.join()
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.loop.close()

    def __run_polling(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        task = self.loop.create_task(self.__polling_thread(), name='PollingThreadTask')
        self.loop.run_until_complete(task)

    async def __polling_thread(self):
        while self.manager.run:
            await self.__poll_for_updates()
            await asyncio.sleep(0.01)

        # __ read updates with next_id in order to avoid to read "end" again in the following run __
        try:
            await self.manager.telegram_bot.get_updates(offset=self.manager.next_id)
        except Exception as e:
            LOGGER.warning(f"Exception during shutdown polling: {e}")

    async def __poll_for_updates(self):
        try:
            updates = await self.manager.telegram_bot.get_updates(offset=self.manager.next_id)
            for update in updates:
                await self.__handle_update(update=update.to_dict())
        except Exception as e:
            LOGGER.warning(f"Exception during polling updates: {e}")

    async def __handle_update(self, update):
        self.manager.next_id = update["update_id"] + 1

        if 'message' in update:
            await self.__handle_message_update(update=update)

        elif 'callback_query' in update:
            self.__handle_callback_update(update=update)

    async def __handle_message_update(self, update):
        message = update['message']
        text = message['text']
        chat_id = message["chat"]["id"]
        is_command = message['text'][0] == '/'
        text = text[1:] if is_command else text

        if text.upper() in {'TEST_TELEGRAM', 'TEST TELEGRAM', 'TESTTELEGRAM'}:
            return await self.manager.telegram_bot.send_message(chat_id=chat_id, text='TELEGRAM IS WORKING')

        from_user = message.get("from", {})
        return self.manager.update_stream.put(
            TelegramMessage(
                message_type=TelegramMessageType.COMMAND if is_command else TelegramMessageType.MESSAGE,
                chat_id=chat_id,
                message_id=message.get("message_id"),
                date=message.get('date'),
                update_id=update['update_id'],
                from_id=from_user.get("id"),
                from_name=from_user.get("first_name"),
                from_username=from_user.get("username"),
                chat_last_name=message["chat"].get("last_name"),
                text=text))

    def __handle_callback_update(self, update):
        callback = update['callback_query']
        message = callback.get('message', {})
        chat = message.get('chat', {})
        from_user = callback.get("from", {})

        return self.manager.update_stream.put(
            TelegramMessage(
                message_type=TelegramMessageType.CALLBACK,
                chat_id=chat.get("id"),
                message_id=message.get("message_id"),
                date=message.get('date'),
                update_id=update['update_id'],
                from_id=from_user.get("id"),
                from_name=from_user.get("first_name"),
                from_username=from_user.get("username"),
                chat_last_name=chat.get("last_name"),
                callback_id=callback['id'],
                data=callback.get('data')))

