"""
this script is to generate training data set for LogReg benchmark
"""
from util.EmbedScript import run_script, ScriptException

if __name__ == "__main__":
    try:
        script = """
set -eu
# setup data set dir
cur_dir="`pwd`"
dir_name='/training-data-set/'
training_name="$0"
ip_size="$1"
set_size_pow="$2"
if [ ! -d $cur_dir$dir_name ]
then
    mkdir $cur_dir$dir_name
    echo "created dir: $cur_dir$dir_name"
fi
if [ ! -d $cur_dir$dir_name$training_name ]
then
    mkdir $cur_dir$dir_name$training_name
    echo "created dir: $cur_dir$dir_name$training_name"
fi
echo "input size: "$ip_size
echo "set pow size: "$set_size_pow
        """
        # args to the script:
        #   $0: name for the training set (e.g.: sin: the data set should show characteristic of sin function)
        #   $1: number of input to the sigmoid neuron
        #   $2: indicate number of tuples in the data-set:
        #       range: 3 ~ 14
        #       e.g.:
        #           suppose user provide 10, then data set with size 2^3, 2^4, 2^5, ... 2^10 will be generated,
        #           and be stored as separate files in the corresponding folder
        stdout, stderr = run_script(script, ['AttenSin', '3', '12'])
        print("============================")
        print("script msg: \n" + str(stdout))
        print("============================")
    except ScriptException as se:
        print(se)
