"""
version 0 of logistic regression:
    doing regression on single layer logistic neurons
    mainly using numpy.array to facilitate data manipulation
    ignoring bias for now
"""

from pyspark import SparkContext
import numpy as np
import operator as op
import os
import argparse

sc = SparkContext(appName="logistic regression v0")
sc.addPyFile(os.path.join(os.path.dirname(__file__), "util/CalcFunc.py"))
sc.addPyFile(os.path.join(os.path.dirname(__file__), "share/conf.py"))
"""
Take a note of this!!!
you don't need to import util.CalcFunc (just do `import CalcFunc`), cuz even though CalcFunc is in 
util dir originally, Spark will put it in the same dir as this file when it "fetches" all required files
==> essentially, all files are at the same level before main function is actually executed.
"""
import CalcFunc as cf
from conf import trainingDirName, dataSetSizeStart

class Conf(tuple):
    """
    Immutable class stores all configuration related to the 
    training of the logistic neural net

    Attributes:
        wInit       numpy.array         the initial weight
        tInit       int                 the initial bias (threshold)
        learnRate   float               the learning rate of weigh update (i.e.: learnRate * gradient = delta(w))
        itrMax      int                 max # of iterations for (logistic) regression
    """
    __slots__ = []
    # not sure if this is the best way to make it immutable
    def __new__(cls, wLen, wInit=[], tInit=0, learnRate=0.01, itrMax=1000):
        assert itrMax > 0 and wLen > 0 and learnRate > 0
        wInit = [float(x) for x in wInit]
        selfWInit = np.array(len(wInit) and wInit or ([0.0] * wLen))
        selfTInit = tInit
        selfItrMax = itrMax
        selfLearnRate = learnRate
        return tuple.__new__(cls, (selfWInit, selfTInit, selfItrMax, selfLearnRate))

    wInit = property(op.itemgetter(0))
    tInit = property(op.itemgetter(1))
    itrMax = property(op.itemgetter(2))
    learnRate = property(op.itemgetter(3))


class Data:
    """
    a class containing all the training data points:
    for each data point, 
    -- there should be a length n list representing input;
    -- there should also be a single y representing target
    
    Attributes:
        pts     [y, numpy.array([x1, x2, ..., xn])]
    """
    def __init__(self, testFileName, sc):
        self.validateInput(testFileName)
        self.setupDataVector(testFileName, sc)

    def validateInput(self, testFileName):
        # TODO: check if input file follows the required format
        pass

    def setupDataVector(self, testFileName, sc):
        """
        this function reads in data from "testFileName",
        (which for now should contain only data points, 
        and should be in the format of: y x1 x2 ... xn)
        parse it to setup the big object of Point
        """
        self.pts = sc.textFile(testFileName) \
                .map(lambda x: x.split()) \
                .map(lambda x: [float(x[0]), \
                np.array([float(x[i + 1]) for i in range(len(x) - 1)])])


def parseArg():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', type=str, metavar='PATHTODATASET',
            required=True, help="file name of training data set")
    parser.add_argument('-i', '--iteration', type=int, metavar='ITR',
            default=100, help="max num of itr to do the log reg")
    parser.add_argument('-w', '--weight', type=float, metavar='W',
            default=0., help='initial weight')
    parser.add_argument('-b', '--bias', type=float, metavar='B',
            default=0., help='initial bias (threshold)')
    parser.add_argument('-r', '--rate', type=float, metavar='R',
            default=0.01, help='learning rate for updating weight')
    return parser.parse_args()


if __name__ == "__main__":
    args = parseArg()
    data = Data(os.path.join(os.path.dirname(__file__), args.file), sc)
    # TODO: should put persist here or in Data.setupDataVector ??
    data.pts.persist()
    wLen = len(data.pts.first()[1])
    conf = Conf(wLen, wInit=[args.weight]*wLen, tInit=args.bias,
            learnRate=args.rate, itrMax=args.iteration)

    print("setting for this run\n{}".format(args))
    print("first data point is:\n{}".format(data.pts.first()))
    print("initial weight vector is:\n{}".format(conf.wInit))
    print("initial threshold is:\n{}".format(conf.tInit))
    
    weight = conf.wInit
    threshold = conf.tInit
    cost = data.pts.map(lambda p: \
        cf.costFunctionSqrErr(p[1], p[0], weight)) \
        .reduce(lambda c1, c2: c1 + c2)

    print("Initial cost is:\n{}".format(cost))

    for itr in range(conf.itrMax):
        if (itr % 10 == 0):
            cost = data.pts.map(lambda p: \
                cf.costFunctionSqrErr(p[1], p[0], weight)) \
                .reduce(lambda c1, c2: c1 + c2)
            print("Itr {}".format(itr))
            print("Cost {}".format(cost))
        gradientW = data.pts.map(lambda p: \
                cf.descentGradientW(p[1], p[0], weight)) \
                .reduce(lambda d1, d2: d1 + d2)
        gradientT = data.pts.map(lambda p: \
                cf.descentGradientT(p[1], p[0], weight)) \
                .reduce(lambda d1, d2: d1 + d2)
        weight += conf.learnRate * gradientW
        threshold += conf.learnRate * gradientT

    print("Final weight: {}".format(weight))
    print("final threshold: {}".format(threshold))
    print("final gradient W: {}".format(gradientW))
    print("final gradient T: {}".format(gradientT))
    print("final itr {}".format(itr))
    print("final cost {}".format(cost))
