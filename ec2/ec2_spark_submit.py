"""
call this script after launched cluster using ec2_spark_launcher.py

this will submit applications to the ec2 cluster
"""

from util.EmbedScript import *
import util.logging as log
import argparse
import re
import json
import pdb



DEFAULT_CREDENTIAL = '../EC2-credential/zimplex0-credentials.csv'
DEFAULT_IDENTITY = '../EC2-credential/zimplex0-key-pair-ap-southeast-1.pem'
DEFAULT_REGION = 'ap-southeast-1'
DEFAULT_CLUSTER_NAME = 'unnamed_cluster'

OUTPUT_FORMAT = 'json'  # don't change this

# all dir are info on the Amazon AWS, not the local machine that this script resides
# directories keywords:
#       https / http: internet
#       file: ec2 (master node) (has to be absolute path)
#       hdfs: aws hadoop
AWS_DIR_INFO = {    #default value
        'spark': '/root/spark/',
        'log': '/root/bm-log/',
        'data': 'file:///root/spark-benchmark-python/LogReg/training-data-set/Sigmoid_in-3-out-1/'
}

# some additional information about the application
APP_INFO = {    # default value
        'repo_url': 'https://github.com/ZimpleX/spark-benchmark-python.git', # will be cloned to under root of master node
        'name': 'LogReg',
        'submit_main': 'LogReg/LogisticRegression.py'   # relative to the newly downloaded repo dir (e.g.: relative to /root/spark-benchmark-python/)
}



def parseArgs():
    parser = argparse.ArgumentParser('submit applications to already launched clusters')
    parser.add_argument('-c', '--credential_file', type=str, metavar='CREDENTIALS', 
            default=DEFAULT_CREDENTIAL, help='file location ec2 user credential')
    parser.add_argument('-i', '--identity_file', type=str, metavar='IDENTITY',
            default=DEFAULT_IDENTITY, help='identity file to ec2, usually <identity>.pem')
    parser.add_argument('-r', '--region', type=str, metavar='REGION', 
            default=DEFAULT_REGION, help='region where clusters located in')
    parser.add_argument('-n', '--cluster_name', type=str, metavar='CLUSTER_NAME', 
            default=DEFAULT_CLUSTER_NAME, help='name of the ec2 cluster')

    return parser.parse_args()



if __name__ == '__main__':
    args = parseArgs()
    # setup cli settings
    try:
        scriptGetKey = """
            set -eu
            credential_file={}
            echo $(cat $credential_file | awk 'NR==2' | awk -F ',' '{{print $(NF-1)}}')
            echo $(cat $credential_file | awk 'NR==2' | awk -F ',' '{{print $NF}}')
        """.format(args.credential_file)
        stdout, stderr = runScript(scriptGetKey, [], output_opt='pipe')
        key_id, secret_key = stdout.decode('utf-8').split('\n')[:-1]
        stdout, stderr = runScript('aws configure', [], output_opt='display', 
                input_opt='pipe', input_pipe=[key_id, secret_key, args.region, OUTPUT_FORMAT])
        print()
        log.printf('AWS conf done', type='INFO')

    except ScriptException as se:
        print(se)

    #########################################
    #  find master id, then get public-dns  #
    #########################################
    master_dns = ''
    try:
        stdout, stderr = runScript('aws ec2 describe-instances', [], output_opt='pipe')
        #inst = json.loads(stdout.decode('utf-8'))
        log.printf('instance info got, target: {}-master' \
                .format(args.cluster_name), type='INFO')
        out_str = stdout.decode('utf-8')
        master_id_regex = '{}-master-{}'.format(args.cluster_name, '\S*')
        master_id = re.search(master_id_regex, out_str)
        if not master_id:
            log.printf('failed to get master-id:\n        check your cluster name / region ...', type='ERROR')
            exit()
        master_id = master_id.group().split('master-')[-1][:-2]
        stdout, stderr = runScript('aws ec2 describe-instances --instance-ids {}'.format(master_id), [], output_opt='pipe')
        out_str = stdout.decode('utf-8')
        master_dns_regex = '"{}": "{}",'.format('PublicDnsName', '\S*')
        master_dns = re.search(master_dns_regex, out_str).group().split("\"")[-2]
        log.printf("Get {}-master public DNS:\n       {}" \
                .format(args.cluster_name, master_dns), type='INFO')
    except ScriptException as se:
        print(se)

    #################################################
    #  login to master node and prepare for submit  #
    #################################################
    try:
        app_root = APP_INFO['repo_url'].split('/')[-1].split('.git')[0]
        submit_main = '/root/{}/{}'.format(app_root, APP_INFO['submit_main'])
        log_dir = '{}{}/'.format(AWS_DIR_INFO['log'], APP_INFO['name'])
        pipeCreateDir = """
            git clone {0}
            
            # pre-submit check
            . .bashrc   # set env var
            py3_path=$(which python3)
            if [[ -z $py3_path ]] || [[ $py3_path =~ '/which:' ]]
            then
                echo 'python3 is not installed! quit.'
                exit
            fi    

            submit_main={1}
            launch_dir=$(pwd)
            log_dir={2}
            spark_dir={3}
            data_file={4}
            master_dns={5}

            echo $launch_dir
            if [ ! -d ${{launch_dir}}/bm-log ]
            then
                mkdir ${{launch_dir}}/bm-log
            fi
            if [ ! -d ${{launch_dir}}/bm-log/LogReg ]
            then
                mkdir ${{launch_dir}}/bm-log/LogReg
            fi

            #############################
            cd spark-benchmark-python/  #
            #############################
            python3 -m util.data_generator

            ############
            cd /root/  #
            ############
            ./spark-ec2/copy-dir spark-benchmark-python


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
            echo $data_file
            echo $master_dns
            echo $submit_main
            PYSPARK_PYTHON=$(which python3) ./bin/spark-submit --master spark://$master_dns:7077 $submit_main -f $data_file

            logout
        """.format(APP_INFO['repo_url'], submit_main, log_dir,
                AWS_DIR_INFO['spark'], AWS_DIR_INFO['data']+'08', master_dns)
        """
        # TODO: need to replace " with \" in pipeCreateDir
        pipeCreateDir = pipeCreateDir.replace('"', '\\\"')
        pipeCreateDir = pipeCreateDir.replace('`', '\\`')
        pipeCreateDir = pipeCreateDir.replace('$(', '\\$(')
        """
        stdout, stderr = runScript('python3 -m ec2.ec2_spark_launcher --login {} --pipe' \
                .format(args.cluster_name), [], 
                output_opt='display', input_opt='pipe', input_pipe=[pipeCreateDir, '.quit'])
    except ScriptException as se:
        print(se)
