"""
launch this from root dir

Basically, this is a wrapper for spark-ec2 script.
you can use this script to launch / login / destroy ec2 clusters. 
commands for customized cluster configuration can also be passed in (piped to spark-ec2 script).
"""

from util.EmbedScript import *
import util.logging as log
import argparse
import pdb

DEFAULT_CREDENTIAL = '../EC2-credential/zimplex0-credentials.csv'
CHILD_SCRIPT = 'ec2/spark-ec2'
DEFAULT_IDENTITY = '../EC2-credential/zimplex0-key-pair-ap-southeast-1.pem'
DEFAULT_SPARK = '../spark-1.5.0-bin-hadoop2.6/'

DEFAULT_NAME = 'unnamed_cluster'

DEFAULT_EC2_ARGS = {'--instance-type': 't2.micro',
                    '--region': 'ap-southeast-1',
                    '--ami': 'ami-94ccdfc6'}
DEFAULT_LAUNCH_ARGS = ['--instance-type', 
                       '--region',
                       '--ami']
DEFAULT_LOGIN_ARGS = ['--region']
DEFAULT_DESTROY_ARGS = ['--region']


def parseArgs():
    parser = argparse.ArgumentParser('launch spark in EC2, wrapper of spark-ec2 script')
    parser.add_argument('-c', '--credential_file', type=str, metavar='CREDENTIALS', 
            default=DEFAULT_CREDENTIAL, help='file location ec2 user credential')
    parser.add_argument('-i', '--identity_file', type=str, metavar='IDENTITY',
            default=DEFAULT_IDENTITY, help='identity file to ec2, usually <identity>.pem')
    parser.add_argument('-s', '--spark_dir', type=str, metavar='SPARK_DIR',
            default=DEFAULT_SPARK, help='location of spark dir')
    parser.add_argument('-seh', '--spark_ec2_help', action='store_true',
            help='help msg from the spark-ec2 script')
    parser.add_argument('-sef', '--spark_ec2_flag', type=str, metavar='SPARK_EC2_FLAG', default='',
            help='flags passed to spark-ec2 script (wrap by "" or \'\') \
                \n[NOTE]: don\'t pass credential file and identity file using -sef; \
                          pass them with -c or -i \
                \n[NOTE]: don\'t contain \'=\' in the arg string')
    parser.add_argument('--launch', type=str, metavar='CLUSTER_NAME', nargs='?',
            const=DEFAULT_NAME, help='launch a ec2 cluster of name <CLUSTER_NAME>')
    parser.add_argument('--login', type=str, metavar='CLUSTER_NAME', nargs='?',
            const=DEFAULT_NAME, help='login to a cluster')
    parser.add_argument('--destroy', type=str, metavar='CLUSTER_NAME', nargs='?',
            const=DEFAULT_NAME, help='destroy cluster, data UNRECOVERABLE!')
    parser.add_argument('--pipe', action='store_true', 
            help='[only for login]: do you want to pipe the input to ec2 master node terminal? \
                    \nwill prompt out an interactive shell to record all cmds to be piped to ec2 shell')
    
    return parser.parse_args()




if __name__ == '__main__':
    args = parseArgs()
    ec2_spark_script = args.spark_dir + CHILD_SCRIPT
    if args.spark_ec2_help:
        try:
            print('\nhelp msg from spark-ec2 script:\n')
            stdout, stderr = runScript('{} -h'.format(ec2_spark_script), [], output_opt='display')
        except ScriptException as se:
            print(se)
        exit()

    mode_keys = None
    if args.launch:
        mode_keys = DEFAULT_LAUNCH_ARGS
    elif args.login:
        mode_keys = DEFAULT_LOGIN_ARGS
    elif args.destroy:
        mode_keys = DEFAULT_DESTROY_ARGS
    else:
        log.printf('unknown mode!', type='ERROR')
        exit()

    second_lvl_arg_dict = {k:DEFAULT_EC2_ARGS[k] for k in mode_keys}
    second_lvl_arg_list = map(lambda i: '{} {}'.format(list(second_lvl_arg_dict.keys())[i], 
                        list(second_lvl_arg_dict.values())[i]), range(len(second_lvl_arg_dict)))
    # NOTE: don't use '+=', cuz if the same arg is overwritten in cmd line, 
    # you need to put the default value prior to the overwritten one
    args.spark_ec2_flag = ' {} {}'.format(' '.join(second_lvl_arg_list), args.spark_ec2_flag)
    args.spark_ec2_flag += ' -i {} -k {}' \
        .format(args.identity_file, args.identity_file.split('.pem')[0].split('/')[-1])
    print('args to spark-ec2 script: \n\t{}'.format(args.spark_ec2_flag))

    ec2_credential = args.credential_file
    aws_access_key_id = None
    aws_secret_access_key = None

    # set environment variable
    try:
        scriptPrep = """
            set -eu
            credential_file={}
            ACCESS_KEY_ID=$(cat $credential_file | awk 'NR==2' | awk -F ',' '{{print $(NF-1)}}')
            SECRET_ACCESS_KEY=$(cat $credential_file | awk 'NR==2' | awk -F ',' '{{print $NF}}')
            export AWS_SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY
            export AWS_ACCESS_KEY_ID=$ACCESS_KEY_ID
            echo $ACCESS_KEY_ID
            echo $SECRET_ACCESS_KEY
        """.format(args.credential_file)
        stdout, stderr = runScript(scriptPrep, [])
        aws_access_key_id = stdout.decode('utf-8').split("\n")[0]
        aws_secret_access_key = stdout.decode('utf-8').split("\n")[1]
        log.printf('environment variable set.')
    except ScriptException as se:
        print(se)

    # launch / login / destroy cluster
    if args.launch:
        try:
            stdout, stderr = runScript('{} {} launch {}' \
                    .format(ec2_spark_script, args.spark_ec2_flag, args.launch), 
                    [], output_opt='display')
            log.printf('cluster successfully launched.', type='INFO')
        except ScriptException as se:
            print(se)
    elif args.login:
        try:
            ip_opt = args.pipe and 'pipe' or 'cmd'
            ip_pipe = []
            if args.pipe:
                print('enter cmds you want to send to ec2 cluster. type \'.quit\' to finish up.')
                while True:
                    new_ip = input('>> ')
                    print(new_ip)
                    if new_ip != '.quit':
                        ip_pipe += [new_ip]
                    else:
                        break
            stdout, stderr = runScript('{} {} login {}' \
                    .format(ec2_spark_script, args.spark_ec2_flag, args.login),
                    [], output_opt='display', input_opt=ip_opt, input_pipe=ip_pipe)
            log.printf('finish interaction with master node.', type='INFO')
        except ScriptException as se:
            print(se)
    elif args.destroy:
        # there has already been warning in spark-ec2 script, so don't prompt warning here
        try:
            stdout, stderr = runScript('{} {} destroy {}' \
                    .format(ec2_spark_script, args.spark_ec2_flag, args.destroy),
                    [], output_opt='display', input_opt='cmd')
            log.printf('cluster successfully destroyed.', type='INFO')
        except ScriptException as se:
            print(se)


