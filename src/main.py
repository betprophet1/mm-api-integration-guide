import argparse

from src import mm_calls
from src.log import logging

parser = argparse.ArgumentParser(description='MM API integration guide')
parser.add_argument('--autobet', action='store_true')


if __name__ == '__main__':
    logging.info("testing MM api")
    args = parser.parse_args()
    
    mm_instance = mm_calls.MMInteractions()
    mm_instance.mm_login()
    mm_instance.get_balance()
    mm_instance.subscribe()
    if args.autobet:
        mm_instance.seeding()
        mm_instance.auto_betting()
    mm_instance.keep_alive()
