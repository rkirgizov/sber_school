#!/usr/bin/env python3
import logging
from logging.handlers import RotatingFileHandler

class KelaLogger:

    def __init__(self, file, only_file=False):
        """Constructor"""
        self.file = file

    def get_file_handler(self):
        file_handler = logging.FileHandler(self.file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - [%(levelname)s] - (%(filename)s).%(funcName)s(%(lineno)d) %(message)s', '%Y-%m-%d %H:%M:%S'))
        return file_handler

    def get_stream_handler(self):
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(logging.Formatter('%(message)s', '%H:%M:%S'))
        return stream_handler
    
    # def get_rotating_file_handler(self):
    #     rotating_file_handler = RotatingFileHandler(self.file, maxBytes=3000, backupCount=5)
    #     return rotating_file_handler
    
    def get_logger(self, name, only_file=False):
        logger = logging.getLogger(name)
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            # logger.addHandler(self.get_rotating_file_handler())
            logger.addHandler(self.get_file_handler())
            if not only_file:
                logger.addHandler(self.get_stream_handler())
        return logger

# Только в файл
# log_f = KelaLogger('kela.log').get_logger(__name__, False)
# log_f.warning('warning')
# log_f.info('info')