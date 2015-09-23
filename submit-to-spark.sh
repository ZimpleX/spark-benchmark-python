#!/bin/bash
set -eu

#############################################################
#   NOTE:                                                   #
#   this script is currently hard coding many variables.    #
#   (cuz now I am only focusing on single benchmark, and    #
#   single training set configuration)                      #
#   Will eventually generalize it.                          #
#############################################################

# policy for logging:
# read-only
# and do renaming every time, based on the file creation time (newer file gets larger index)
# -t flag is doing the ranking based on creation time
# -r for reverse the ordering of ls (newest file is at the bottom of ls output)

launch_dir="`pwd`"
log_dir=$launch_dir"/../bm-log/LogReg/"
spark_dir=$launch_dir"/../spark-1.5.0-bin-hadoop2.6/"

if [ ! -d "$launch_dir/../bm-log" ]
then
    echo "created log dir: $launch_dir/../bm-log"
    mkdir $launch_dir/../bm-log
fi
if [ ! -d "$launch_dir/../bm-log/LogReg" ]
then
    echo "created log dir: $launch_dir/../bm-log/LogReg"
    mkdir $launch_dir/../bm-log/LogReg
fi

dir_format(){
    #################
    cd $log_dir     #
    #################
    # dealing with renaming
    file_set="`ls -tr`"
    file_counter="1"
    for file in $file_set
    do
        mv $file temp-$file_counter
        file_counter=$((file_counter+1))
    done

    file_set="`ls -tr`"
    file_counter="1"
    for file in $file_set
    do
        mv $file $file_counter".out"
        file_counter=$((file_counter+1))
    done

    count="`ls -ltr | wc -l`"
    #################
    cd $spark_dir   #
    #################
}

dir_format

#./sbin/start-master.sh
#./sbin/start-slave.sh spark://zimplex:7077

data_set_dir=$launch_dir'/LogReg/training-data-set/Sigmoid/'
for size in {03..12}
do
    file_name=${data_set_dir}3_${size}
    ./bin/spark-submit --master spark://zimplex:7077 $launch_dir/LogReg/LogisticRegression.py -f $file_name | tee $log_dir/${count}.out
    chmod 444 $log_dir/*
    dir_format
done 

#./sbin/stop-all.sh

