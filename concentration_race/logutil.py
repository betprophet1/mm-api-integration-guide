"""Shared logging setup for concentration_race scenarios.

src/log.py's basicConfig attaches a console StreamHandler at INFO to the
root logger, so every logging.info() call in deduce_tests.py (wager
placements, balance breakdowns) prints to the terminal by default. Route
that noise to a file instead and keep the console to our own print()
summaries plus warnings/errors.
"""
import logging


def quiet_console_log_to_file(log_file):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    root_logger.addHandler(file_handler)

    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and handler is not file_handler:
            handler.setLevel(logging.WARNING)
