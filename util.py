# -*- coding:utf-8 -*-

import os
import time
import logging
from config import LOG_DIR, LOG_NAME_PREFIX, FILE_LOG_LEVEL, CONSOLE_LOG_LEVEL


time.sleep(2)

log_file_path = LOG_DIR + LOG_NAME_PREFIX + time.strftime('%Y-%m-%d-%H-%M-%S')
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)

# create formatter
fmtstr = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(fmtstr)

logging.basicConfig(format=fmtstr, filename=log_file_path, datefmt='%a, %d %b %Y %H:%M:%S')

# create logger
logger = logging.getLogger('UPLOAD')
logger.setLevel(FILE_LOG_LEVEL)

# create console handler and set level to debug
console_hanlder = logging.StreamHandler()
console_hanlder.setLevel(CONSOLE_LOG_LEVEL)

# add formatter to console_hanlder
console_hanlder.setFormatter(formatter)
# add console_hanlder to logger
logger.addHandler(console_hanlder)

## 'test' code
#logger.debug('debug message')
#logger.info('info message')
#logger.warn('warn message')
#logger.error('error message')
#logger.critical('critical message')

