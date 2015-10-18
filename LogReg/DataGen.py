"""
this script is to generate training data set for LogReg benchmark
it uses a mix of shell script and python script, as i think shell script is
more concise with regard to directory formatting

also note that the policy regarding data set is:
    never overwrite a data set file again --> to ensure that different runs of benchmark use the same data set
"""
from __future__ import print_function
from share.conf import trainingDirName, dataSetSizeStart
from util.EmbedScript import runScript, ScriptException
from util.TrainingFunc import trainingFunc
from random import uniform
import os
import argparse

def parseArg():
    parser = argparse.ArgumentParser()
    parser.add_argument("-is", "--input_size", type=int, metavar='M', 
            choices=range(1, 11), default=3, 
            help="specify the num of input to the neuron")
    parser.add_argument("-ds", "--data_size", type=int, metavar='N', 
            choices=range(3, 15), default=12, 
            help="specify the size of data set (in terms of 2^N)")
    parser.add_argument("-f", "--function", type=str, 
            choices=['Sigmoid', 'AttenSin1', 'Random', 'AttenSin2', 'AttenSin3'], default='Sigmoid', 
            help="Specify the training function to gen the data set")
    return parser.parse_args()


if __name__ == "__main__":
    ####################
    #    parse args    #
    ####################
    args = parseArg()
    taskName = args.function
    inputSize = args.input_size
    dataSetSize = args.data_size
    assert inputSize > 0 and inputSize <= 10
    assert dataSetSize <= 14 and dataSetSize >= 3
    #################################
    #    format the data set dir    #
    #################################
    try:
        # shell script to be run as subprocess
        scriptFormatDir = """
            set -eu
            # setup data set dir
            orig_dir="`pwd`"
            dir_name='/training-data-set/'
            task_name="$0"
            ip_size="$1"
            set_size_pow="$2"
            set_size_pow_start=0"$3"
            if [ ! -d $orig_dir$dir_name ]
            then
                mkdir $orig_dir$dir_name
                echo "created dir: $orig_dir$dir_name"
            fi
            if [ ! -d $orig_dir$dir_name$task_name ]
            then
                mkdir $orig_dir$dir_name$task_name
                echo "created dir: $orig_dir$dir_name$task_name"
            fi
            echo "input size: "$ip_size
            echo "set pow size: "$set_size_pow

            task_dir=$orig_dir$dir_name$task_name
            ##################
            cd $task_dir     #
            ##################
            existing_set="`ls`"
            for size_pow in $(eval echo "{$set_size_pow_start..$set_size_pow}")
            do
                file_name=$ip_size'_'$size_pow
                if [ "`find . -maxdepth 1 -type f -printf '%f\n' | grep $file_name`" ]
                then 
                    echo "data set already exists: "$file_name    
                else
                    touch $file_name
                    echo "created data set file: "$file_name
                fi
            done
            ##################
            cd $orig_dir     #
            ##################
        """
        # args to the script:
        #   $0: name for the training set (e.g.: sin: the data set should show characteristic of sin function)
        #   $1: number of input to the sigmoid neuron
        #   $2: indicate number of tuples in the data-set:
        #       range: 3 ~ 14
        #       e.g.:
        #           suppose user provide 10, then data set with size 2^3, 2^4, 2^5, ... 2^10 will be generated,
        #           and be stored as separate files in the corresponding folder
        #   $3: indicate min number of tuples in the data-set
        stdout, stderr = runScript(scriptFormatDir, [taskName, str(inputSize), str(dataSetSize), str(dataSetSizeStart)])
        print("============================")
        print("script msg: \n" + str(stdout))
        print("============================")
    except ScriptException as se:
        print(se)


    #########################################
    #    generate data and write to file    #
    #########################################
    genY = trainingFunc(taskName)
    for dFile in os.listdir(trainingDirName + taskName):
        dFileFull = trainingDirName + taskName + '/' + dFile
        # all old files should already be read-only
        if not os.access(dFileFull, os.W_OK):
            continue

        ipSize, dSize = dFile.split("_")
        ipSize = int(ipSize)
        dSize = int(dSize)
        assert ipSize == inputSize
        assert dSize >= dataSetSizeStart and dSize <= dataSetSize
        assert os.stat(dFileFull).st_size == 0
        f = open(dFileFull, 'w')
        numEntry = pow(2, dSize)
        for i in range(0, numEntry):
            # randomly generate input list, within range 0 ~ 10
            xList = [uniform(0, 10) for k in range(0, inputSize)]
            dataList = [genY(xList)] + xList
            dataStr = reduce(lambda x,y: str(x)+' '+str(y), dataList)
            print(dataStr, file=f)
        f.close()
    

    ##########################################################
    #    always enforce read-only policy for data set dir    #
    ##########################################################
    try:
        scriptChmod = """
            orig_dir="`pwd`"
            dir_name='/training-data-set/'
            task_name="$0"
            chmod 444 $orig_dir$dir_name$task_name/*
        """
        stdout, stderr = runScript(scriptChmod, [taskName])
    except ScriptException as se:
        print(se)
