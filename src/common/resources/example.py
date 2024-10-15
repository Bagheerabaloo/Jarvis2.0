import asyncio
from telegram import Bot


async def main(token: str):
    # Initialize the bot with your bot token
    bot = Bot(token)

    print("Bot started. Listening for updates...")

    # Keep polling for updates
    while True:
        # Get updates from the chat
        updates = await bot.get_updates()

        # Print each update
        for update in updates:
            print(update)

        # Wait for a short interval before polling again
        await asyncio.sleep(1)


# Avviamo il programma principale utilizzando asyncio
if __name__ == '__main__':
    asyncio.run(main(token="521932309:AAEBJrGFDznMH1GEiH5veKRR3p787_hV_2w"))
