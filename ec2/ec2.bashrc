# modified .bashrc for ec2 cluster running spark
# added some env var for easy submission of benchmarks
#
#####################################
#  to be copied to ec2 master node  #
#####################################

# User specific aliases and functions

alias rm='rm -i'
alias cp='cp -i'
alias mv='mv -i'

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# added by Anaconda3 2.3.0 installer
export PATH="/root/anaconda3/bin:$PATH"

# for easy test spark cluster
export SP_SUBMIT='/root/spark/bin/spark-submit'
export PYSPARK_PYTHON=$(which python3)
export SP_TEST_PY_MAIN='/root/spark-benchmark-python/LogReg/LogisticRegression.py'
export SP_MASTER=$(cat /root/spark-ec2/cluster-url)
