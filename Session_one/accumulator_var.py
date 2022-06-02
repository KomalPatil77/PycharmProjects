from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]")\
        .appName("accumulator variable")\
        .getOrCreate()

# accumulator variable

    # accvar = spark.sparkContext.accumulator(1)
    # rdd = spark.sparkContext.parallelize([20,30,40,50])
    #
    # def fun(x):
    #     accvar.add(x)
    #
    # rdd.foreach(fun)
    #print("acc value:" + str(accvar.value))


# broadcast variable: Readonly variable

    states = {"MH": "Maharashtra", "MP": "Madhyapradesh", "GJ": "Gujarat"}
    broadcast_states = spark.sparkContext.broadcast(states)
    inputrdd = spark.sparkContext.textFile("C:\\Users\\KOMAL\\AppData\\Local\\Programs\\Python\\Python310\\Scripts\\file.txt")
    #print(inputrdd.collect())

    def changestateval(x):
        xsplit = x.split(",") #return list
        testlist = []
        for i in xsplit:
            try:
                i = broadcast_states.value[i]
            except:
                pass
            testlist.append(i)
        return ','.join(testlist)  #return string comma separeted

    print(inputrdd.map(lambda x: changestateval(x)).collect())
    #print(outputrdd.collect())


