"""
working with python3

formatting the [INFO], [WARN] and [ERROR] printed out 
by self-written script, the same way as Spark
"""
from functools import reduce
import pdb

def printf(string, *reflex, type='INFO', separator=None):
    """
    type can be INFO / WARN / ERROR
    separator can be anything, e.g.: '=', '-', '*'
    """
    if reflex:
        string = string.format(reflex)
    string = '[{}]\t{}'.format(type, string)
    if not separator:
        print(string)
    else:
        maxLen = reduce(lambda l1,l2: (len(l2) > len(l1)) and len(l2) or len(l1), string.split("\n"), '')
        sepLine = separator*maxLen
        print('{}\n{}\n{}'.format(sepLine, string, sepLine))
