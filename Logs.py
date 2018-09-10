# @Author  : SandyHeng
# @File    : Logs
# @Software: PyCharm
import os
import datetime

from config import LOG_DIR


class Logs(object):

    def __init__(self):
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
        self.__log_handle = None

    def open_log_file(self, file="{}{}.log".format(LOG_DIR, datetime.datetime.now().strftime("%Y-%m-%d"))):
        self.__log_handle = open(file, 'a')

    def write_log(self, msg):
        if not self.__log_handle:
            self.open_log_file()
        self.__log_handle.write('{}\t\t{}\n'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))
        self.__log_handle.flush()

    def close_log_file(self):
        self.__log_handle.close()
