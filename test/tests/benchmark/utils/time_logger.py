import os
import time
import json

"""
Class to manage time logger file
"""
class timeLogger(object):
    def __init__(self, path):
        self.__path = path
        self.__str_config = 'configuration'
        self.__str_time = 'time marker'

    def __write_case_info(self, catalog, key, value):
        if not os.path.exists(self.__path):
            os.makedirs(self.__path)

        try:
            f = open(os.path.join(self.__path, 'case_info.log'), 'r+')
        except IOError:
            # Create a new file
            f = open(os.path.join(self.__path, 'case_info.log'), 'a+')

        try:
            json_data = json.load(f)
        except ValueError:
            json_data = {}

        if not json_data.has_key(catalog):
            json_data[catalog] = {}

        json_data[catalog][key] = value
        f.seek(0)
        f.write(json.dumps(json_data, indent=4, separators=(',',': ')))
        f.truncate()
        f.close()

    def __get_current_time(self):
        return time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(time.time()))

    def write_interval(self, interval):
        self.__write_case_info(self.__str_config, 'interval', interval)

    def write_start(self):
        self.__write_case_info(self.__str_time, 'start', self.__get_current_time())

    def write_end(self):
        self.__write_case_info(self.__str_time, 'end', self.__get_current_time())

    def write_event(self, key):
        self.__write_case_info(self.__str_time, key, self.__get_current_time())
