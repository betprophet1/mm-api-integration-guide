import asyncio

from src import config, mm_calls
from src.log import logging


async def main():
    logging.info("testing MM api")
    mm_instance = mm_calls.MMInteractions(config.WEBSOCKET_SERVICE)
    mm_instance.mm_login()
    mm_instance.get_balance()
    await mm_instance.subscribe()
    mm_instance.seeding()
    await mm_instance.ably_binding_channels()  # for ably only
    mm_instance.auto_betting()
    await mm_instance.keep_alive()

asyncio.run(main())
