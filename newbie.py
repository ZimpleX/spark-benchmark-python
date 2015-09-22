from pyspark import SparkContext

sc = SparkContext("local", "newbie")
textFile = sc.textFile("README.md")

print("\n\n")
print(textFile.count())
print("\n\n")
