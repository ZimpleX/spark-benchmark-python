"""
1. run with python3
2. parse all profiling value from each stage of the task
3. all the parsed data are of standard unit:
    - time: s
    - memory: B
"""

from bs4 import BeautifulSoup
import urllib.request as urlreq
import argparse
import util.populate_db as popDb

##########
#  META  #
##########

# PROFILE_TABLE_CONF = ['appID', 'benchmark', 'workerNum', 'dataSize', 'nodeType']
UNDEFINED = -1

DEFAULTDATAMAX = 26

PUBLICURL = 'http://{}:8080/'   # format it in argparser
APPIDLIST = []
BENCHMARKLIST = []
TOTALDURATIONLIST = []
NUMWORKERS = -1
INSTANCETYPE = ''
JOBSUBDIR = 'history/{}/jobs/'     # no use now (we are counting stage-wise)
STAGESUBDIR = 'history/{}/stages/'  # format with app-id
STAGEDETAILSUBDIR = 'stage/?id={}&attempt=0' # append this to STAGESUBDIR

# this is no use (I am reading metric name directly from web UI)
METRIC_NAME = [ 'Duration',         # computing time
                'Scheduler Delay',   # IO delay ??
                'Task Deserialization Time',
                'GC Time',
                'Result Serialization Time',
                'Getting Result Time',
                'Peak Execution Memory',
                'Input Size',
                'Shuffle Read Size',
                'Shuffle Read Blocked Time',
                'Shuffle Remote Reads']        # NOTE: i am not considering Record metric currently

METRIC_PERCENT = [0, 25, 50, 75, 100]

###########################
#  profiler table design  #
###########################

# appID serve as the primary key to join tables
TABLE_META_DATA = [ '[app_ID]', '[instance_type]', '[num_worker]', '[benchmark]', 
                    '[data_size]', '[total_duration_(sec)]']
TABLE_DETAIL1_DATA = ['[app_ID]', '[stage_ID]', '[metric_name]', '[metric_val_(sec/byte)]', '[value_percent(%)]']    # design metric name to be column rather than row to facilitate plotting
TABLE_DETAIL2_DATA = ['[app_ID]', '[stage_ID]', '[executor_max_ID]', '[executor_failed]']


TABLE_META_TYPE = ['TEXT', 'TEXT', 'INTEGER', 'TEXT', 'INTEGER', 'REAL']
TABLE_DETAIL1_TYPE = ['TEXT', 'INTEGER', 'TEXT', 'REAL', 'INTEGER']
TABLE_DETAIL2_TYPE = ['TEXT', 'INTEGER', 'INTEGER', 'INTEGER']


################
#  arg parser  #
################

def parseArg():
    parser = argparse.ArgumentParser("parse data into db from spark web UI of clusters")
    parser.add_argument('-u', '--url', type=str, metavar='URL',
            help="public URL of master. NO NEED to provide protocal (http://) and port (8080)")
    parser.add_argument('-a', '--appID', type=str, metavar='APPID',
            default='', help="app id to parse. Can be found on web UI")
    parser.add_argument('-ns', '--recentAppStart', type=int, metavar='RECENTAPPSTART',
            default=0, help="parse the most recent apps from .. (the entry in the web UI table is 0)\n(this argument will only take effect when appID is not provided)")
    parser.add_argument('-ne', '--recentAppEnd', type=int, metavar='RECENTAPPEND',
            default=0, help="parse the most recent apps until ..")
    parser.add_argument('-i', '--instanceType', type=str, metavar='INSTANCETYPE',
            default='t2.large', help="type of EC2 instance of the cluster")
    return parser.parse_args()


##########
#  Util  #
##########

def formatTime(timeStr):
    """
    format time to be 'second':
        recognize 'ms' / 'mS' / 'min' / 'Min'
    """
    unit = timeStr.split()[-1]
    val = float(timeStr.split()[0])
    if unit == 'ms' or unit == 'mS':
        return val*0.001
    elif unit == 'min' or unit == 'Min':
        return val*60.
    elif unit == 'h':
        return val*3600.
    else:
        return val


##########
#  main  #
##########

if __name__ == '__main__':
    args = parseArg()
    INSTANCETYPE = args.instanceType
    PUBLICURL = PUBLICURL.format(args.url)
    # get num workers
    r_pub = urlreq.urlopen(PUBLICURL)
    soup_pub = BeautifulSoup(r_pub)
    NUMWORKERS = int(soup_pub.find_all('ul', class_='unstyled')[0].findAll('li')[2].get_text().split(':')[-1])
    # setup app list and app name list
    if len(args.appID) != 0:
        APPIDLIST = list(args.appID)
        BENCHMARKLIST = APPIDLIST   # TODO: change this
    else:
        APPIDLIST = []
        BENCHMARKLIST = []
        TOTALDURATIONLIST = []
        for appNum in range(args.recentAppStart, args.recentAppEnd+1):
            tempApp = soup_pub.find_all('table', class_='table table-bordered table-condensed table-striped sortable')[-1] \
                                .tbody.findAll('tr')[appNum]
            APPIDLIST += [tempApp.findAll('td')[0].a.get_text()]
            BENCHMARKLIST += [tempApp.findAll('td')[1].a.get_text()]
            TOTALDURATIONLIST += [formatTime(tempApp.findAll('td')[-1].get_text())]

    print('num workers: {}'.format(NUMWORKERS))
    print('appID: {}'.format(APPIDLIST))
    print('bmID: {}'.format(BENCHMARKLIST))

    appCount = 0
    curDSizeFromDefault = DEFAULTDATAMAX

    for app_id in APPIDLIST:
        ###############
        #  enter app  #
        ###############
        stageMetaURL = PUBLICURL + STAGESUBDIR.format(app_id)
        curBenchmark = BENCHMARKLIST[appCount]
        curDataSize = -1
        if len(curBenchmark.split('-size-')) == 2:
            curDataSize = int(curBenchmark.split('-size-')[-1][0:2])
            curBenchmark = curBenchmark.split('-size-')[0] + curBenchmark.split('-size-')[-1][2:]
        else:
            curDataSize = curDSizeFromDefault
            curDSizeFromDefault -= 1
        curTotDur = TOTALDURATIONLIST[appCount]
        # populate meta table
        tableEntryMeta = [app_id, INSTANCETYPE, NUMWORKERS, curBenchmark, curDataSize, curTotDur]
        print('tableEntryMeta: {}'.format(tableEntryMeta))
        popDb.populate_db(TABLE_META_DATA, TABLE_META_TYPE, tableEntryMeta, table_name='meta|meta')
        print('meta: {}'.format(tableEntryMeta))
        # get total num of stages
        r = urlreq.urlopen(stageMetaURL)
        soup = BeautifulSoup(r)
        numStages = int(soup.find_all('li', id='completed-summary')[0].get_text().split(':')[-1])

        stageDetailURL = stageMetaURL + STAGEDETAILSUBDIR
        print(stageDetailURL)
        preIdMax = -1
        for stage_id in range(0, numStages):
            #################
            #  enter stage  #
            #################
            print('stage: {}'.format(stage_id))
            r = urlreq.urlopen(stageDetailURL.format(stage_id))
            soup = BeautifulSoup(r)
            soup_table = soup.find_all("table", id="task-summary-table")[0]
            
            allMetric = soup_table.findAll('tr')

            tableEntryDetail1_pre = [app_id, stage_id]
            tableEntryDetail2_pre = [app_id, stage_id]
            # populate detail2 table
            soup_exe = soup.find_all('table', class_='table table-bordered table-condensed table-striped')[0].tbody.findAll('tr')
            curIdMax = UNDEFINED
            curFailed = UNDEFINED
            for s_exe in soup_exe:
                curId = int(s_exe.findAll('td')[0].get_text())
                print('exe: {}'.format(s_exe))
                curIdMax = curId > curIdMax and curId or curIdMax
            if preIdMax == UNDEFINED:
                curFailed = 0
            else:
                curFailed = curIdMax - preIdMax
            preIdMax = curIdMax
            tableEntryDetail2 = tableEntryDetail2_pre + [curIdMax, curFailed]
            popDb.populate_db(TABLE_DETAIL2_DATA, TABLE_DETAIL2_TYPE, tableEntryDetail2, table_name='detail2|detail2')

            for metric in allMetric:
                ##################
                #  sweep metric  #
                ##################
                metricName = metric.findAll('td')[0].get_text()
                metric = metric.findAll('td')[1:]
                valPerCount = -1    # keep record of what percent is the current metric value
                for val_txt in metric:
                    valPerCount += 1
                    val = val_txt.get_text().split()[0]
                    unit = val_txt.get_text().split()[1]    # the list can be more than 2 elements
                    val = formatTime('{} {}'.format(val, unit))
                    if unit[-1] == 'B':
                        if unit[0] == 'k' or unit[0] == 'K':
                            val *= 1000.
                        elif unit[0] == 'm' or unit[0] == 'M':
                            val *= 1000000.
                        elif unit[0] == 'g' or unit[0] == 'G':
                            val *= 1000000000.
                    # populate detail1 table
                    tableEntryDetail1 = tableEntryDetail1_pre + [metricName, val, METRIC_PERCENT[valPerCount]]
                    popDb.populate_db(TABLE_DETAIL1_DATA, TABLE_DETAIL1_TYPE, tableEntryDetail1, table_name='detail1|detail1')
                    # print("val: {}".format(val))
        appCount += 1
