import argparse

from . import mm_calls
from .log import logging

if __name__ == '__main__':
    logging.info("testing MM api")

    try:
        mm_instance = mm_calls.MMInteractions()
        mm_instance.mm_login()
        
        # Call the test_batch_cancel_422 method to trigger a 422 error
        logging.info("Calling test_batch_cancel_422 to trigger a 422 error")
        success, status_code, error_msg = mm_instance.test_batch_cancel_422()
        logging.info(f"Test result - Status: {status_code}, Error: {error_msg}")
        
        # Uncomment below lines if you want to run the regular flow
        # mm_instance.get_balance()
        # mm_instance.seeding()
        # mm_instance.subscribe()  # subscribe to various public and private channels
        # mm_instance.auto_playing()
        # mm_instance.keep_alive()
    except Exception as e:
        logging.error(f"Error: {str(e)}")
