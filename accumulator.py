from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="acc-test-0")

    pseudo_ac = 0;
    ac = sc.accumulator(0)

    l = [1,2,3,4,5]

    def add_ac(n): 
        global pseudo_ac
        pseudo_ac += 1
        ac.add(1)

    sc.parallelize(l).foreach(add_ac)

    #if __name__ == "__main__":
    print("pseudo_ac: " + str(pseudo_ac))
    print("ac: " + str(ac))
    sc.stop()
