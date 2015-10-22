"""
working with python3

formatting the [INFO], [WARN] and [ERROR] printed out 
by self-written script, the same way as Spark
"""
from functools import reduce
import pdb

def printf(string, *reflex, type='INFO', separator='default'):
    """
    type can be INFO / WARN / ERROR
    separator can be anything, e.g.: '=', '-', '*' or None
    if separator == 'default', then will use:
        '-' for 'INFO',
        '=' for 'WARN',
        '*' for 'ERROR'
        '%' for others
    """
    if separator == 'default':
        separator = '%'
        separator = (type=='INFO') and '-' or separator
        separator = (type=='WARN') and '=' or separator
        separator = (type=='ERROR') and '*' or separator
    if reflex:
        string = string.format(reflex)
    string = '[{}] {}'.format(type, string)
    if not separator:
        print(string)
    else:
        maxLen = reduce(lambda l1,strL2: (l1 < len(strL2)) and len(strL2) or l1, string.split("\n"), 0)
        sepLine = separator*maxLen
        print('{}\n{}\n{}\n'.format(sepLine, string, sepLine))
