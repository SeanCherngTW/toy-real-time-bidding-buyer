import os
import logging
from os.path import exists
from logging import handlers


class DebugLog(object):
    def __init__(self, ad_path_config):
        self.model_name = ad_path_config['model_name']
        self.log_file_path = ad_path_config['log_file_path'] + self.model_name + ".log"
        self.dst_dir = ad_path_config['dst_dir']
        self.prepare_log_path()
        self.logger = self.logger_initialize()
        self.logger.propagate = False

    def prepare_log_path(self):
        if not os.path.exists(self.dst_dir):
            os.mkdir(self.dst_dir)

    def logger_initialize(self):
        logger = logging.getLogger(self.model_name)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '[%(asctime)s] - [%(name)s] - [%(filename)s] - %(levelname)s - %(message)s'
        )
        fh = handlers.RotatingFileHandler(
            filename=self.log_file_path,
            backupCount=1,
            encoding="utf-8",
        )
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger
