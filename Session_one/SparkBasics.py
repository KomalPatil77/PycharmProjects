from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # print("Hello Class....!")
    # initialize SparkConf
    # sparkconf = SparkConf().setAppName("Spark Context init").setMaster("local[*]")
    # define SparkContext
    # sc = SparkContext(conf=sparkconf)
    # print(sc)

    spark = SparkSession.builder.appName("Spark Context Init")\
        .config("spark.driver.bindAddress", "localhost")\
        .config("spark.Ui.port", "4050")\
        .master("local[*]").getOrCreate()
    #print(spark)

    inputrdd = spark.sparkContext.textFile("C:\\Users\\KOMAL\\AppData\\Local\\Programs\\Python\\Python310\\Scripts\\inputfile.txt")
    outputrdd = inputrdd.map(lambda x: x.split(","))
    #.map(lambda x: (x[4] ,1)).reduceByKey(lambda x, y: x+y)
    #print(outputrdd.collect())
    outputrdd.map(lambda x: (x[5], int(x[6]))).reduceByKey(lambda x,y:x+y)
    print(outputrdd.collect())




# setAppName: current Application name
# We are using spark in standalone mode then master is local
# [*] indicates no of executors
# A class name initial letter is capital
# setAppName: is a method. initial letter is small
# getOrCreate: sparksession is already exist the get it or it is not exist then create it
