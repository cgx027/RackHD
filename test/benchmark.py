from proboscis import register
from proboscis import TestProgram
import sys
import getopt

def run_tests(api_version):

    if api_version == '2':
        import tests.benchmark.api_v2_0 as benchmark
    else:
        import tests.benchmark.api_v1_1 as benchmark

    register(groups=['poller'], depends_on_groups=benchmark.poller_tests)
    register(groups=['discovery'], depends_on_groups=benchmark.discovery_tests)
    register(groups=['bootstrap'], depends_on_groups=benchmark.bootstrap_tests)

    TestProgram().run_and_exit()


if __name__ == '__main__':

    # Generate the name of the directory that stores the benchmark data
    api_version = "1"

    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["api_version=","group=="])
        for op, value in opts:
            if op == "--api_version":
                # Remove this arg from array to prevent TestProgram processing it
                sys.argv = filter(lambda x: x != op+'='+value, sys.argv)
                api_version = value
            if op == "-h":
                print "Usage: benchmark.py [--api_version|--group]"
                exit()
    except getopt.GetoptError:
        sys.exit("option or arg is not supported")

    run_tests(api_version)
