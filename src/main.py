import asyncio

from src import mm_calls
from src.log import logging


async def main():
    logging.info("testing MM api")
    mm_instance = mm_calls.MMInteractions()
    mm_instance.mm_login()
    mm_instance.get_balance()
    await mm_instance.subscribe()
    mm_instance.seeding()
    mm_instance.auto_betting()
    await mm_instance.keep_alive()

asyncio.run(main())
