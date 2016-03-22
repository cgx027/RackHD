from config.api1_1_config import *
from config.amqp import *
from modules.logger import Log
from modules.amqp import AMQPWorker
from on_http_api1_1 import NodesApi as Nodes
from on_http_api1_1 import WorkflowApi as Workflows
from on_http_api1_1 import rest
from datetime import datetime
from proboscis.asserts import assert_equal
from proboscis.asserts import assert_not_equal
from proboscis import SkipTest
from proboscis import test
from json import loads
import time

from tests.benchmark import ansible_ctl
from tests.benchmark.utils.data_display import dataDisplay
from tests.benchmark.utils.time_logger import timeLogger

LOG = Log(__name__)

@test(groups=['poller.tests'])
class PollerTests(object):
    def __init__(self):
        self.__client = config.api_client
        self.__worker = None
        self.__discovery_duration = None
        self.__discovered = 0
        self.__testname = 'poller'
        ansible_ctl.render_case_name(self.__testname)
        self.__data_path = ansible_ctl.get_data_path_per_case()
        self.__time_logger = timeLogger(self.__data_path)

    def start_daemon(self):
        return ansible_ctl.run_playbook('start_daemon.yml')

    def collect_data(self):
        return ansible_ctl.run_playbook('collect_data.yml')

    def check_compute_count(self):
        Nodes().nodes_get()
        nodes = loads(self.__client.last_response.data)
        count = 0
        for n in nodes:
            type = n.get('type')
            if type == 'compute':
                count += 1
        return count

    @test(groups=['poller.pre.tests'])
    def test_check_precondition(self):
        """ Testing precondition fulfilled """
        if self.check_compute_count():
            LOG.info('Nodes already discovered!')
            self.check_pollers()
            return
        self.__discovery_duration = datetime.now()
        LOG.info('Wait start time: {0}'.format(self.__discovery_duration))
        self.__worker = AMQPWorker(queue=QUEUE_GRAPH_FINISH,callbacks=[self.handle_graph_finish])
        self.__worker.start()

    def handle_graph_finish(self,body,message):
        routeId = message.delivery_info.get('routing_key').split('graph.finished.')[1]
        Workflows().workflows_get()
        workflows = loads(self.__client.last_response.data)
        for w in workflows:
            definition = w['definition']
            injectableName = definition.get('injectableName')
            if injectableName == 'Graph.SKU.Discovery':
                graphId = w['context'].get('graphId')
                if graphId == routeId:
                    status = body.get('status')
                    if status == 'succeeded':
                        options = definition.get('options')
                        nodeid = options['defaults'].get('nodeId')
                        duration = datetime.now() - self.__discovery_duration
                        LOG.info('{0} - target: {1}, status: {2}, route: {3}, duration: {4}'
                                .format(injectableName,nodeid,status,routeId,duration))
                        self.__discovered += 1
                        message.ack()
                        break
        check = self.check_compute_count()
        if check and check == self.__discovered:
            self.__worker.stop()
            self.__worker = None
            self.__discovered = 0
            self.check_pollers()

    def check_pollers(self):
        Nodes().nodes_get()
        nodes = loads(self.__client.last_response.data)

        for n in nodes:
            if n.get('type') == 'compute':
                uuid = n.get('id')
                Nodes().nodes_identifier_pollers_get(uuid)
                assert_not_equal(0, len(loads(self.__client.last_response.data)),\
                    message='Pollers are not working!')
        LOG.info('Pollers are working')

    def get_current_time(self):
        return time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(time.time()))

    @test(groups=['test_poller'], depends_on_groups=['poller.pre.tests'])
    def test_runtime(self):
        """ Testing footprint scenario: poller """
        self.__time_logger.write_interval(ansible_ctl.get_data_interval())
        self.__time_logger.write_start()

        assert_equal(True, self.start_daemon(), message='Failed to start data collection daemon!')

        # Run test scenario
        # In this case, wait for 15 mins to let RackHD run pollers
        LOG.info('Start test case...')
        time.sleep(900)
        LOG.info('End test case. Fetch log...')

        assert_equal(True, self.collect_data(), message='Failed to collect footprint data!')
        self.__time_logger.write_end()

        dataDisplay(self.__data_path).generate_graph()
