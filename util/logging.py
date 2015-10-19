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
    string = '[{}] {}'.format(type, string)
    pdb.set_trace()
    if not separator:
        print(string)
    else:
        maxLen = reduce(lambda l1,strL2: (l1 < len(strL2)) and len(strL2) or l1, string.split("\n"), 0)
        sepLine = separator*maxLen
        print('{}\n{}\n{}'.format(sepLine, string, sepLine))
