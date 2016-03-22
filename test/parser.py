import os
import re
import datetime
import json

filename_atop = "cpu_mem_net_disk.log"
filename_process = "pid.log"
filename_caseinfo = "case_info.log"
filename_summary = "summary.log"
filename_summary_js = "summary.js"
filename_mongo_document = "mongo_document.log"
filename_mongo_document_js = "mongo_document.js"
filename_mongo_disk = "mongo_disk.log"

EXPECTED_COLUMN_CNT = 15

LINE_COL = [
    "PID",
    "SYSCPU",
    "USRCPU",
    "VSIZE",
    "RSIZE",
    "RDDSK",
    "WRDSK",
    "RNET",
    "SNET",
    "RNETBW",
    "RNETBW_U",
    "SNETBW",
    "SNETBW_U",
    "CPU"
]

MATRIX_COL = [
    "SYSCPU",
    "USRCPU",
    "VSIZE",
    "RSIZE",
    "RDDSK",
    "WRDSK",
    "RNET",
    "SNET",
    "RNETBW",
    "SNETBW",
    "CPU"
]

MONGO_COL = [
    "dataSize",
    "storageSize"
]

def parse_mongo_document(input_file):
    mongo_disk_records = {}
    for matrix in MONGO_COL:
        mongo_disk_records[matrix] = []

    with open(input_file, 'r') as f:
        for line in f:
            for matrix in MONGO_COL:
                if line.find(matrix) >= 0 :
                    splited_line = split_line_by_colon(line)
                    mongo_disk_records[matrix].append(int(splited_line[1]))
                    break
    return mongo_disk_records

def parse_case_info(filename):
    with open(filename, 'r') as f:
        case_info_json = json.load(f)
    # print(case_info_json)
    return case_info_json

def parse_cpu_time(time):
    #return number of micro second
    # time may be '12m53s', or '0.01s'
    hour_match = re.findall('\d+h', time)
    minute_match = re.findall('\d+m', time)
    sec_match = re.findall('[0-9]+\.*[0-9]*s',time)

    if len(hour_match) == 0:
        hour = 0
    else:
        hour = int(hour_match[0][:-1])

    if len(minute_match) == 0:
        minute = 0
    else:
        minute = int(minute_match[0][:-1])

    if len(sec_match) == 0:
        sec = 0
    else:
        sec = float(sec_match[0][:-1])

    # Return time in unit of ms (microsecond)
    time_ret = int((sec + (minute * 60) + (hour * 3600)) * 1000)
    return time_ret

def parse_size(size):
    # return size of byte of memory usage, or disk usage
    # size may be '1.2T', '1.3G', '348.2M', '95488K', or '0K'
    size_match = re.findall('[0-9]+\.*[0-9]*[TGMK]',size)
    size_str = size_match[0]

    scale = 1
    if size_str[-1] == 'T':
        scale = 1000000000000
    elif size_str[-1] == 'G':
        scale = 1000000000
    elif size_str[-1] == 'M':
        scale = 1000000
    elif size_str[-1] == 'K':
        scale = 1000

    size = float(size_str[:-1])
    # Return time in unit of BYTE
    size_ret = int(size * scale)
    # print(size_str + ' scale: ' + str(scale) + ' ' + str(size_ret))
    return  size_ret

def parse_network_io(io):
    # return the amount of network io activity
    # size may be '7515', '104e4', or '989e3'
    io_match = re.findall('(?P<base>[0-9]+)e*(?P<exponent>[0-9]*)', io)
    io_str_base = io_match[0][0]
    io_str_exponent = io_match[0][1]

    io_base = int(io_str_base)
    if io_str_exponent == '':
        io_exponent = 0
    else:
        io_exponent = int(io_str_exponent)

    # Return number of IOs
    io_ret = io_base * pow(10, io_exponent)
    # print('str:' + io + ' base:' + io_str_base + ' exp:' + io_str_exponent + ' ret:' + str(io_ret))
    return io_ret

def parse_network_bw(bw, unit):
    # return the network bandwidth in unit of bps (byte per second)
    # bw may be '13' or '989'
    # unit may be 'Kbps', 'Mbps', 'Gbps' or 'Tbps'

    scale = 1
    if unit == 'Tbps':
        scale = 1000000000000
    elif unit == 'Gbps':
        scale = 1000000000
    elif unit == 'Mbps':
        scale = 1000000
    elif unit == 'Kbps':
        scale = 1000

    # Return bandwidth in unit of bps (byte per second)
    bw_ret = int(bw) * scale
    # print('bw:' + bw + ' unit:' + unit + ' ret:' + str(bw_ret))
    return bw_ret

# parse summary data from file, and write to js file
def parse_summary(filename, process_list):
    summary_list = {}
    with open(filename, 'r') as f:
        for line in f:
            if line.find('ATOP') < 0:
                parsed_line = parse_line_atop(line)

                pid = parsed_line['process']
                process_name = get_process_name(process_list, pid)

                if process_name not in summary_list:
                    summary_list[process_name] = []

                summary_list[process_name].append(parsed_line['list'])

    for process in summary_list.keys():
        delta = [int(y) - int(x) for x, y in zip(summary_list[process][0], summary_list[process][1])]
        summary_list[process] = delta

    return summary_list

def write_summary_to_js(summary_data, statistic_atop, statistic_mongo, output_filename):
    cwd = os.path.dirname(os.path.realpath(__file__))
    file_dir_name = os.path.join(cwd, output_filename)
    file_open = open(file_dir_name, 'w')

    for process in summary_data.keys():
        for matrix in MATRIX_COL:
            for statistic in statistic_atop[process][matrix]:
                file_open.write(process + '_' + matrix.lower() + '_' + statistic
                                + ' = '
                                + str(statistic_atop[process][matrix][statistic])
                                + '\n')
            file_open.write('\n')
        file_open.write('\n')

    for matrix in statistic_mongo.keys():
        for statistic in statistic_mongo[matrix]:
            file_open.write('mongodb_' + matrix.lower() + '_' + statistic
                                    + ' = '
                                    + str(statistic_mongo[matrix][statistic])
                                    + '\n')
        file_open.write('\n')
    file_open.write('\n')

def calc_max_min_avg_atop(matrix_data):
    max_min_avg_ret = {}
    for process in matrix_data.keys():
        max_min_avg_ret[process] = {}
        matrix_list = {}

        for matrix in MATRIX_COL:
            matrix_list[matrix] = []

        records = matrix_data[process]

        for record in records:
            for matrix in MATRIX_COL:
                matrix_list[matrix].append(record[MATRIX_COL.index(matrix)])

        for matrix in MATRIX_COL:
            max_min_avg_ret[process][matrix] = calc_statistic(matrix_list[matrix])

    return max_min_avg_ret

def calc_max_min_avg_mongo(matrix_data):
    max_min_avg_ret = {}
    for matrix in matrix_data.keys():
        max_min_avg_ret[matrix] = calc_statistic(matrix_data[matrix])
    return max_min_avg_ret

def calc_statistic(list_data):
    ret_val = {}

    ret_val["max"] = max(list_data)
    ret_val["min"] = min(list_data)
    summation = sum(list_data)
    ret_val["sum"] = summation
    ret_val["avg"] = summation/(float(len(list_data)))

    return ret_val

def write_matrix_to_js(matrix_data, starttime_str, sample_interval):
    start_time = datetime.datetime.strptime(starttime_str, "%Y/%m/%d %H:%M:%S")

    matrix_list = {}
    padding_str = ',\\n\" + \n'

    pid_name_list = matrix_data.keys()
    pid_name_list_str = ",".join(sorted(pid_name_list))

    record_length_list = []
    for pid,pid_record in matrix_data.items():
        record_length_list.append(len(pid_record))

    record_cnt = min(record_length_list)

    # Get output files ready, one per each matrix
    cwd = os.path.dirname(os.path.realpath(__file__))
    for matrix_idx in range(len(MATRIX_COL)):    # No need to loop the PID matrix
        matrix_value = MATRIX_COL[matrix_idx].lower()
        filename = matrix_value + '.js'
        file_dir_name = os.path.join(cwd, filename)
        file_open = open(file_dir_name, 'w')
        matrix_list[matrix_value] = file_open

        # write headers
        file_open.write(matrix_value + '_' + 'data = \n')
        file_open.write('\"Time,' + pid_name_list_str + padding_str)

        for record in range(record_cnt):     # Remove the first record
            line_records = []
            for pid in sorted(pid_name_list):
                # print(pid + ' ' + str(record)+ ' ' + ' '+ str(matrix_idx))
                line_records.append(str(matrix_data[pid][record][matrix_idx]))
            if record == (record_cnt - 1):
                padding = ',\"'
            else:
                padding = padding_str

            current_time = start_time + datetime.timedelta(seconds = record * sample_interval)
            current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
            line = "\"" + current_time_str + ',' + ",".join(line_records) + padding
            file_open.write(line)

def write_mongo_doc_to_js(matrix_data, starttime_str, sample_interval, filename):
    start_time = datetime.datetime.strptime(starttime_str, "%Y/%m/%d %H:%M:%S")

    padding_str = ',\\n\" + \n'

    matrix_name_list = matrix_data.keys()
    matrix_name_list_str = ",".join(sorted(matrix_name_list))

    cwd = os.path.dirname(os.path.realpath(__file__))
    file_dir_name = os.path.join(cwd, filename)
    file_open = open(file_dir_name, 'w')

    # write headers
    file_open.write('mongo_document' + '_' + 'data = \n')
    file_open.write('\"Time,' + matrix_name_list_str + padding_str)

    record_length_list = []
    for matrix, records in matrix_data.items():
        record_length_list.append(len(records))

    record_cnt = min(record_length_list)

    for record in range(record_cnt):
        line_records = []
        for matrix in sorted(matrix_name_list):
            line_records.append(str(matrix_data[matrix][record]))

        if record == (record_cnt - 1):
            padding = ',\"'
        else:
            padding = padding_str

        current_time = start_time + datetime.timedelta(seconds = record * sample_interval)
        current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
        line = "\"" + current_time_str + ',' + ",".join(line_records) + padding
        file_open.write(line)

def split_line_by_space(line):
    splited_line = re.split("\s+", line)
    #remove empty element
    while '' in splited_line:
        splited_line.remove('')
    return splited_line

def split_line_by_colon(line):
    splited_line = re.split(":", line)
    #remove empty element
    while '' in splited_line:
        splited_line.remove('')
    splited_line[1] = splited_line[1][0:-2] # remove the tailing ',\n'
    return splited_line

def parse_process_list(filename):
    PROCESS_LIST = [
    "on-http",
    "on-syslog",
    "on-taskgraph",
    "on-tftp",
    "on-dhcp-proxy",
    "beam.smp",
    "mongod",
    "dhcpd"
    ]
    processes = {}
    with open(filename, 'r') as f:
        for line in f:
            param_list = split_line_by_space(line)

            for process in PROCESS_LIST:
                if line.find(process) >= 0:
                    processes[param_list[0]] = process
                    break

        return processes

def get_process_name(process_dict, pid_str):
    return process_dict[pid_str]

def parse_line_atop(line):
    ret_val = {}
    param_list = split_line_by_space(line)

    # parse PID
    pid_col = LINE_COL.index('PID')
    pid = param_list[pid_col]
    ret_val['process'] = pid

    # Parse SYSCPU
    syscpu_col = LINE_COL.index('SYSCPU')
    syscpu_str = param_list[syscpu_col]
    syscpu = parse_cpu_time(syscpu_str)
    # Parse USRCPU
    usrcpu_col = LINE_COL.index('USRCPU')
    usrcpu_str = param_list[usrcpu_col]
    usrcpu = parse_cpu_time(usrcpu_str)

    # Parse memory VSIZE
    vsize_col = LINE_COL.index('VSIZE')
    vsize_str = param_list[vsize_col]
    vsize = parse_size(vsize_str)

    # Parse memory RSIZE
    rsize_col = LINE_COL.index('RSIZE')
    rsize_str = param_list[rsize_col]
    rsize = parse_size(rsize_str)

    # Parse memory RDDSK
    rddsk_col = LINE_COL.index('RDDSK')
    rddsk_str = param_list[rddsk_col]
    rddsk = parse_size(rddsk_str)

    # Parse memory WRDSK
    wrdsk_col = LINE_COL.index('WRDSK')
    wrdsk_str = param_list[wrdsk_col]
    wrdsk = parse_size(wrdsk_str)

    # Parse network RNET
    rnet_col = LINE_COL.index('RNET')
    rnet_str = param_list[rnet_col]
    rnet = parse_network_io(rnet_str)

    # Parse network SNET
    snet_col = LINE_COL.index('SNET')
    snet_str = param_list[snet_col]
    snet = parse_network_io(snet_str)

    # Parse network RNETBW
    rnetbw_col = LINE_COL.index('RNETBW')
    rnetbw_u_col = LINE_COL.index('RNETBW_U')
    rnetbw_str = param_list[rnetbw_col]
    rnetbw_u_str = param_list[rnetbw_u_col]
    rnetbw = parse_network_bw(rnetbw_str, rnetbw_u_str)

    # Parse network SNETBW
    snetbw_col = LINE_COL.index('SNETBW')
    snetbw_u_col = LINE_COL.index('SNETBW_U')
    snetbw_str = param_list[snetbw_col]
    snetbw_u_str = param_list[snetbw_u_col]
    snetbw = parse_network_bw(snetbw_str, snetbw_u_str)

    # Parse CPU utilization
    cpu_col = LINE_COL.index('CPU')
    cpu_str = param_list[cpu_col]
    cpu = int(cpu_str[0:-1])
    # print('CPU: ' + str(cpu))
    ret_val['list'] = [syscpu, usrcpu, vsize, rsize, rddsk, wrdsk,
                rnet, snet, rnetbw, snetbw, cpu]

    return ret_val

# parse atop log file
def parse_atop(filename, proc_list):
    ret_val = {}
    with open(filename, 'r') as f:
        for line in f:
            parsed_line = parse_line_atop(line)

            pid = parsed_line['process']
            process_name = get_process_name(proc_list, pid)

            if process_name not in ret_val:
                ret_val[process_name] = []
                continue

            ret_val[process_name].append(parsed_line['list'])
    return ret_val

# The main program            
def parse():
    # parse mongodb disk usage
    pathname_mongo_document = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename_mongo_document)
    mongo_document = parse_mongo_document(pathname_mongo_document)

    # parse case info
    pathname_caseinfo = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename_caseinfo)
    case_info = parse_case_info(pathname_caseinfo)

    # parse process list
    pathname_process = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename_process)
    process_list = parse_process_list(filename_process)

    # parse atop log file
    pathname_atop = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename_atop)
    atop_matrix = parse_atop(pathname_atop, process_list)

    # calc max/min/avg from result
    max_min_avg_atop = calc_max_min_avg_atop(atop_matrix)
    max_min_avg_mongo = calc_max_min_avg_mongo(mongo_document)

    # Parse result summary
    pathname_summary = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename_summary)
    summary = parse_summary(pathname_summary, process_list)

    pathname_summary_js = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename_summary_js)
    write_summary_to_js(summary, max_min_avg_atop, max_min_avg_mongo, pathname_summary_js)

    # Print to js log file
    write_matrix_to_js(atop_matrix, case_info["time marker"]["start"], case_info["configuration"]["interval"])
    pathname_mongo_document_js = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename_mongo_document_js)
    write_mongo_doc_to_js(mongo_document,
                          case_info["time marker"]["start"],
                          case_info["configuration"]["interval"],
                          pathname_mongo_document_js)

parse()