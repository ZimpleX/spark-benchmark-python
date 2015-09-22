"""
version 0 of logistic regression:
    doing regression on single layer logistic neurons
    mainly using numpy.array to facilitate data manipulation
    ignoring bias for now
"""

from pyspark import SparkContext
import numpy as np
import math
import operator as op
import os

sc = SparkContext(appName="logistic regression v0")
sc.addPyFile(os.path.join(os.path.dirname(__file__), "util/CalcFunc.py"))
"""
Take a note of this!!!
you don't need to import util.CalcFunc, cuz even though CalcFunc is in util dir originally,
Spark will put it in the same dir as this file when it "fetches" all required files
==> essentially, all files are at the same level before main function is actually executed.
"""
import CalcFunc as cf

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
        assert itrMax > 0 and wLen > 0
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

"""
def actByLog(xList, weight):
    z = np.dot(xList, weight)
    return 1 / (1 + math.exp(-z))
"""


if __name__ == "__main__":
    data = Data(os.path.join(os.path.dirname(__file__), "test-data.ignore.txt"), sc)
    # TODO: should put persist here or in Data.setupDataVector ??
    data.pts.persist()
    conf = Conf(len(data.pts.first()[1]), itrMax=100)

    print("first data point is:")
    print(data.pts.first())
    print("initial weight vector is:")
    print(conf.wInit)
    print("initial threshold is:")
    print(conf.tInit)
    
    weight = conf.wInit
    threshold = conf.tInit
    cost = data.pts.map(lambda p: \
        cf.costFunctionSqrErr(p[1], p[0], weight)) \
        .reduce(lambda c1, c2: c1 + c2)

    print("Initial cost is:")
    print(cost)

    for itr in range(conf.itrMax):
        if (itr % 10 == 0):
            cost = data.pts.map(lambda p: \
                cf.costFunctionSqrErr(p[1], p[0], weight)) \
                .reduce(lambda c1, c2: c1 + c2)
            print("Itr " + str(itr))
            print("Cost " + str(cost))
        gradientW = data.pts.map(lambda p: \
                cf.descentGradientW(p[1], p[0], weight)) \
                .reduce(lambda d1, d2: d1 + d2)
        gradientT = data.pts.map(lambda p: \
                cf.descentGradientT(p[1], p[0], weight)) \
                .reduce(lambda d1, d2: d1 + d2)
        weight += conf.learnRate * gradientW
        threshold += conf.learnRate * gradientT

    print("Final weight:")
    print(weight)
    print("final threshold:")
    print(threshold)
    print("final gradient W:")
    print(gradientW)
    print("final gradient T:")
    print(gradientT)
    print("final itr")
    print(itr)
    print("final cost")
    print(cost)
