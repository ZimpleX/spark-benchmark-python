"""
this module defines all available training functions for generating the training data set
All training function will produce a target value between 0 and 1
"""
from math import exp, sin
from random import uniform

def trainingFunc(funcName):
    """
    first class function: return a closure of the actual training function

    argument:
        funcName        the description of how the training function should behave
    return:
        the actual training function
    """
    if funcName == "Sigmoid":
        """
        this is the very most suitable function for log neurons
        """
        def sigmoid(xList):
            xSum = reduce(lambda x1, x2: x1+x2, xList)
            return 1 / (1 + exp(-xSum))
        return sigmoid
    elif funcName == "AttenSin1":
        def attenSin1(xList):
            assert len(xList) >= 1
            expPow = xList[0] * 0.2
            sinSum = reduce(lambda x1, x2: x1 + x2, xList) - xList[0]
            return exp(-abs(expPow)) * abs(sin(sinSum))
        return attenSin1
    elif funcName == "Random":
        def rand(xList):
            return uniform(0, 1)
        return rand
    elif funcName == "AttenSin2":
        def attenSin2(xList):
            assert len(xList) >= 1
            expPow = xList[0] * 0.2
            sinSum = reduce(lambda x1, x2: x1 + x2, xList) - xList[0]
            return exp(-abs(expPow)) * sin(sinSum)
        return attenSin2
    elif funcName == "AttenSin3":
        def attenSin3(xList):
            sinSum = sum(xList)
            return exp(-0.06 * sinSum) * sin(sinSum)
        return attenSin3

