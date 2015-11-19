"""
    parse all profiling value from each stage of the task
"""

from bs4 import BeautifulSoup
import urllib

METRIC_NAME = [ 'Duration',         # computing time
                'SchedulerDelay',   # IO delay ??
                'TaskDeserializationTime',
                'GCTime',
                'ResultSerializationTime',
                'GettingResultTime',
                'PeakExecutionMemory',
                'InputSize']        # NOTE: i am not considering Record metric currently

METRIC_COLUMN = ['0Percent', '25Percent', '50Percent', '75Percent', '100Percent']

URL_FRAME = 'http://ec2-54-169-145-132.ap-southeast-1.compute.amazonaws.com:8080/history/app-20151119050254-0000/stages/stage/?id={}&attempt=0'
for stage_id in range(0, 1):
    r = urllib.urlopen(URL_FRAME.format(stage_id))
    soup = BeautifulSoup(r)
    soup_t = soup.find_all("table", id="task-summary-table")
    assert len(soup_t) == 1
    soup_table = soup_t[0]
    
    allMetric = soup_table.findAll('tr')
    metric_count = 0
    for metric in allMetric:
        print(METRIC_NAME[metric_count])
        metric = metric.findAll('td')[1:-1]
        for val_txt in metric:
            val = float(val_txt.get_text().split()[0])
            unit = val_txt.get_text().split()[1]    # the list can be more than 2 elements
            if unit[0] == 'm':
                unit = unit[1:-1] + unit[-1]
                val *= 0.001
            elif unit[0] == 'M':
                unit = unit[1:-1] + unit[-1]
                val *= 1000000.
            elif unit[0] == 'k' or unit[0] == 'K':
                unit = unit[1:-1] + unit[-1]
                val *= 1000.
            elif unit[0] == 'G' or unit[0] == 'g':
                unit = unit[1:-1] + unit[-1]
                val *= 1000000000.
            print("val: {} unit: {}".format(val, unit))
        metric_count += 1

