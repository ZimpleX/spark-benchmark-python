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
OUTPUT_FORMAT = 'json'

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
        log.printf('AWS conf done', type='INFO', separator='-')

    except ScriptException as se:
        print(se)
    print()

    # find master id, then get public-dns
    try:
        stdout, stderr = runScript('aws ec2 describe-instances', [], output_opt='pipe')
        #inst = json.loads(stdout.decode('utf-8'))
        log.printf('instance info got', type='INFO', separator='-')
        out_str = stdout.decode('utf-8')
        master_id_regex = '{}-master-{}'.format(args.cluster_name, '\S*')
        master_id = re.search(master_id_regex, out_str).group().split('master-')[-1][:-2]
        stdout, stderr = runScript('aws ec2 describe-instances --instance-ids {}'.format(master_id), [], output_opt='pipe')
        out_str = stdout.decode('utf-8')
        master_dns_regex = '"{}": "{}",'.format('PublicDnsName', '\S*')
        master_dns = re.search(master_dns_regex, out_str).group().split("\"")[-2]
        log.printf("Get master public DNS:\n       {}".format(master_dns), type='INFO', separator='-')
    except ScriptException as se:
        print(se)
