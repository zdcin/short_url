# -*- coding:utf-8 -*-

from config import CONFIG
import logging
from logging.handlers import RotatingFileHandler

# logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger()


def initlog(logfile):
    log = logging.getLogger()
    Rthandler = RotatingFileHandler(logfile, maxBytes=CONFIG['max_log_size'],
                                    backupCount=CONFIG['log_num'])
    formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s', '%y-%m-%d %H:%M:%S')
    Rthandler.setLevel(logging.DEBUG)
    Rthandler.setFormatter(formatter)
    log.addHandler(Rthandler)
    log.setLevel(logging.NOTSET)

    return log
