"""
this script is to generate training data set for LogReg benchmark
"""
from util.EmbedScript import run_script, ScriptException

if __name__ == "__main__":
    taskName = 'AttenSin'
    inputSize = 3
    dataSetSize = 12
    assert inputSize > 0 and inputSize <= 10
    assert dataSetSize <= 14 and dataSetSize >= 3
    try:
        # shell script to be run as subprocess
        script = """
            set -eu
            # setup data set dir
            orig_dir="`pwd`"
            dir_name='/training-data-set/'
            task_name="$0"
            ip_size="$1"
            set_size_pow="$2"
            set_size_pow_start=03
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
        stdout, stderr = run_script(script, [taskName, str(inputSize), str(dataSetSize)])
        print("============================")
        print("script msg: \n" + str(stdout))
        print("============================")
    except ScriptException as se:
        print(se)
