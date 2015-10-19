"""
launch this from root dir

wrapper of spark-ec2 script
"""

from util.EmbedScript import *
import util.logging as log
import argparse

DEFAULT_CREDENTIAL = '../EC2-credential/zimplex0-credentials.csv'
CHILD_SCRIPT = 'ec2/spark-ec2'
DEFAULT_IDENTITY = '../EC2-credential/zimplex0-key-pair-uswest.pem'
DEFAULT_SPARK = '../spark-1.5.0-bin-hadoop2.6/'

DEFAULT_NAME = 'unnamed_cluster'

DEFAULT_SECOND_LVL_ARGS = {'--instance-type': 'm1.large',
                           '--region': 'us-west-2'}


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
                          pass them with -c or -i')
    parser.add_argument('--launch', type=str, metavar='CLUSTER_NAME', nargs='?',
            const=DEFAULT_NAME, help='launch a ec2 cluster of name <CLUSTER_NAME>')
    parser.add_argument('--login', type=str, metavar='CLUSTER_NAME', nargs='?',
            const=DEFAULT_NAME, help='login to a cluster')
    parser.add_argument('--destroy', type=str, metavar='CLUSTER_NAME', nargs='?',
            const=DEFAULT_NAME, help='destroy cluster, data UNRECOVERABLE!')
    
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

    second_lvl_arg_list = map(lambda i: '{} {}'.format(list(DEFAULT_SECOND_LVL_ARGS.keys())[i], 
                        list(DEFAULT_SECOND_LVL_ARGS.values())[i]), range(len(DEFAULT_SECOND_LVL_ARGS)))
    # NOTE: don't use '+=', cuz if the same arg is overwritten in cmd line, 
    # you need to put the default value prior to the overwritten one
    if args.launch:
        args.spark_ec2_flag = ' '.join(second_lvl_arg_list) + args.spark_ec2_flag
    elif args.login:
        args.spark_ec2_flag = ''
    elif args.destroy:
        pass
    else:
        log.printf('unknown mode!', type='ERROR', separator='*')
        exit()
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
            ACCESS_KEY_ID=`cat $credential_file | awk 'NR==2' | awk -F ',' '{{print $(NF-1)}}'`
            SECRET_ACCESS_KEY=`cat $credential_file | awk 'NR==2' | awk -F ',' '{{print $NF}}'`
            export AWS_SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY
            export AWS_ACCESS_KEY_ID=$ACCESS_KEY_ID
            echo $ACCESS_KEY_ID
            echo $SECRET_ACCESS_KEY
        """.format(args.credential_file)
        stdout, stderr = runScript(scriptPrep, [])
        aws_access_key_id = stdout.decode('utf-8').split("\n")[0]
        aws_secret_access_key = stdout.decode('utf-8').split("\n")[1]
        log.printf('environment variable set.', separator='-')
    except ScriptException as se:
        print(se)

    # launch / login / destroy cluster
    if args.launch:
        try:
            stdout, stderr = runScript('{} {} launch {}' \
                    .format(ec2_spark_script, args.spark_ec2_flag, args.launch), 
                    [], output_opt='display')
            log.printf('cluster successfully launched.', type='INFO', separator='-')
        except ScriptException as se:
            print(se)
    elif args.login:
        try:
            stdout, stderr = runScript('{} {} login {}' \
                    .format(ec2_spark_script, args.spark_ec2_flag, args.login),
                    [], output_opt='display')
            log.printf('cluster successfully logged in.', type='INFO', separator='-')
        except ScriptException as se:
            print(se)
    elif args.destroy:
        destroy_warning = "Data won't be recoverable after destroy\nDo you really want to destroy {}? [Y]/n"
        log.printf(destroy_warning, args.destroy, type='WARN', separator='=')
        veri_destroy = input()
        if veri_destroy not in ['y', 'Y']:
            print()
            exit()
        try:
            stdout, stderr = runScript('{} destroy {}' \
                    .format(ec2_spark_script, args.destroy),
                    [], output_opt='display')
            log.printf('cluster successfully destroyed.', type='INFO', separator='-')
        except ScriptException as se:
            print(se)


