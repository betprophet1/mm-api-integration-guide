import argparse

from src import mm_calls
from src.log import logging

if __name__ == '__main__':
    logging.info("testing MM api")

    mm_instance = mm_calls.MMInteractions()
    mm_instance.mm_login()
    mm_instance.get_balance()
    mm_instance.subscribe()
    mm_instance.seeding()
    mm_instance.auto_betting()
    # mm_instance.keep_alive()
