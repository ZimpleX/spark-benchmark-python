"""
A collection of functions related to neural network, including:

Neuron activity:
    logistic (sigmoid) neuron
Squared error cost function
Gradient descent

NOTE that functions in this file nearly all dealing with single
point only. As we would operate on the whole data set using RDD
"""
import numpy as np
import math

def actByLog(xList, weight):
    """
    Neuron activity function.

    arguments:
        xList       input signal vector
        weight      weight vector
    return:
        y           single output signal
    """
    z = np.dot(xList, weight)
    return 1 / (1 + math.exp(-z))

def descentGradientW(x, t, weight):
    """
    calculate the gradient of the cost function W.R.T weight, 
    afterwards, the gradient would be used to update weight

    arguments:
        x       input vector to a single neuron
        t       target output corresponds to x
        weight  current weight of the neuron
    return:
        gradient
    """
    y = actByLog(x, weight)
    return (y * (1 - y) * (t - y)) * x

def descentGradientT(x, t, weight):
    """
    calculate the gradient of the cost function W.R.T threshold, 
    afterwards, the gradient would be used to update threshold

    arguments:
        x       input vector to a single neuron
        t       target output corresponds to x
        weight  current weight of the neuron
    return:
        gradient
    """
    y = actByLog(x, weight)
    return (y * (1 - y) * (t - y))


def costFunctionSqrErr(x, t, weight):
    """
    calculate the squared error between target and neuron output
    arguments:
        x       input vector to a single neuron
        t       target output corresponds to x
        weight  current weight of the neuron
    return:
        squared err of single data point
    """
    y = actByLog(x, weight)
    return 0.5 * pow(y - t, 2)
