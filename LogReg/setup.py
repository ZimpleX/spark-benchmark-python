"""
with reference to: https://pythonhosted.org/an_example_pypi_project/setuptools.html
"""
import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "benchmark for spark: logistic regression",
    version = "0.1",
    author = "Hanson Zeng",
    author_email = "zhqhku@gmail.com",
    description = ("doing logistic regression on a single layer neuron"),
    license = "",
    long_description = read("README")
)
