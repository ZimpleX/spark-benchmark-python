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

# PROFILE_TABLE_CONF = ['appID', 'benchmark', 'workerNum', 'dataSize', 'nodeType']

PUBLICURL = 'http://{}:8080/'   # format it in argparser
APPIDLIST = []
BENCHMARKLIST = []
TOTALDURATIONLIST = []
NUMWORKERS = -1
INSTANCETYPE = ''
JOBSUBDIR = 'history/{}/jobs/'     # no use now (we are counting stage-wise)
STAGESUBDIR = 'history/{}/stages/'  # format with app-id
STAGEDETAILSUBDIR = 'stage/?id={}&attempt=0' # append this to STAGESUBDIR

METRIC_NAME = [ 'Duration',         # computing time
                'SchedulerDelay',   # IO delay ??
                'TaskDeserializationTime',
                'GCTime',
                'ResultSerializationTime',
                'GettingResultTime',
                'PeakExecutionMemory',
                'InputSize']        # NOTE: i am not considering Record metric currently

METRIC_COLUMN = ['0Percent', '25Percent', '50Percent', '75Percent', '100Percent']

PROFILE_TABLE_DATA = ['appID', 'instanceType', 'benchmark', 'numWorker', 'dataSize', 'totalDuration', 'stageID', 
        'Duration0', 'Duration25', 'Duration50', 'Duration75', 'Duration100',
        'SchedulerDelay0', 'SchedulerDelay25', 'SchedulerDelay50', 'SchedulerDelay75', 'SchedulerDelay100',
        'TaskDeserializationTime0', 'TaskDeserializationTime25', 'TaskDeserializationTime50', 'TaskDeserializationTime75', 'TaskDeserializationTime100',
        'GCTime0', 'GCTime25', 'GCTime50', 'GCTime75', 'GCTime100',
        'ResultSerializationTime0', 'ResultSerializationTime25', 'ResultSerializationTime50', 'ResultSerializationTime75', 'ResultSerializationTime100',
        'GettingResultTime0', 'GettingResultTime25', 'GettingResultTime50', 'GettingResultTime75', 'GettingResultTime100',
        'PeakExecutionMemory0', 'PeakExecutionMemory25', 'PeakExecutionMemory50', 'PeakExecutionMemory75', 'PeakExecutionMemory100',
        'InputSize0', 'InputSize25', 'InputSize50', 'InputSize75', 'InputSize100']

PROFILE_TABLE_TYPE = ['TEXT', 'TEXT', 'TEXT', 'INTEGER', 'REAL', 'REAL', 'INTEGER'] + ['REAL']*40

def parseArg():
    parser = argparse.ArgumentParser("parse data into db from spark web UI of clusters")
    parser.add_argument('-u', '--url', type=str, metavar='URL',
            help="public URL of master. NO NEED to provide protocal (http://) and port (8080)")
    parser.add_argument('-a', '--appID', type=str, metavar='APPID',
            default='', help="app id to parse. Can be found on web UI")
    parser.add_argument('-n', '--recentApp', type=int, metavar='RECENTAPP',
            default=1, help="parse the most recent n apps (the top n entries in the web UI table)\n(this argument will only take effect when appID is not provided)")
    parser.add_argument('-i', '--instanceType', type=str, metavar='INSTANCETYPE',
            default='t2.large', help="type of EC2 instance of the cluster")
    return parser.parse_args()


# Util:
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
    else:
        return val



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
        for appNum in range(0, args.recentApp):
            tempApp = soup_pub.find_all('table', class_='table table-bordered table-condensed table-striped sortable')[-1] \
            .tbody.findAll('tr')[appNum]
            APPIDLIST += [tempApp.findAll('td')[0].a.get_text()]
            BENCHMARKLIST += [tempApp.findAll('td')[1].a.get_text()]
            TOTALDURATIONLIST += [formatTime(tempApp.findAll('td')[-1].get_text())]

    print('num workers: {}'.format(NUMWORKERS))
    print('appID: {}'.format(APPIDLIST))
    print('bmID: {}'.format(BENCHMARKLIST))

    appCount = 0
    for app_id in APPIDLIST:
        stageMetaURL = PUBLICURL + STAGESUBDIR.format(app_id)
        curBenchmark = BENCHMARKLIST[appCount]
        curDataSize = int(curBenchmark.split('-size-')[-1][0:2])
        curTotDur = TOTALDURATIONLIST[appCount]
        # get total num of stages
        r = urlreq.urlopen(stageMetaURL)
        soup = BeautifulSoup(r)
        numStages = int(soup.find_all('li', id='completed-summary')[0].get_text().split(':')[-1])

        # 
        stageDetailURL = stageMetaURL + STAGEDETAILSUBDIR
        print(stageDetailURL)
        for stage_id in range(0, numStages):
            r = urlreq.urlopen(stageDetailURL.format(stage_id))
            soup = BeautifulSoup(r)
            soup_t = soup.find_all("table", id="task-summary-table")
            assert len(soup_t) == 1
            soup_table = soup_t[0]
            
            allMetric = soup_table.findAll('tr')
            metric_count = 0

            dataEntry = [app_id, INSTANCETYPE, curBenchmark, NUMWORKERS, curDataSize, curTotDur, stage_id]
            for metric in allMetric:
                print(METRIC_NAME[metric_count])
                metric = metric.findAll('td')[1:]
                for val_txt in metric:
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
                    dataEntry += [val]
                    # print("val: {}".format(val))
                metric_count += 1
            popDb.populate_db(PROFILE_TABLE_DATA, PROFILE_TABLE_TYPE, dataEntry)
        appCount += 1
