#!/usr/bin/env python3
"""
This module defines a function which implement a
flexible event logging system for the hillview
"""
# pylint: disable=invalid-name

import logging

def get_logger(module_name):
    """ Returns the logger object """
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.DEBUG)
    con_log_handler = logging.StreamHandler()
    con_log_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    con_log_handler.setFormatter(formatter)
    logger.addHandler(con_log_handler)
    return logger
