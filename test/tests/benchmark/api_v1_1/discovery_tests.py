from config.api1_1_config import *
from config.amqp import *
from modules.logger import Log

from proboscis import test

LOG = Log(__name__)

@test(groups=['discovery.tests'])
class DiscoveryTests(object):
    def __init__(self):
        self.__client = config.api_client
        self.__worker = None
        self.__discovery_duration = None
        self.__discovered = 0
        self.__testname = 'discovery'

    @test(groups=['discovery.pre.tests'])
    def test_check_precondition(self):
        """ Testing discovery precondition fulfilled """
        pass

