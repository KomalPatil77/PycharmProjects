from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Context Init") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.Ui.port", "4050") \
        .master("local[*]").getOrCreate()
    print(spark)

    inputrdd = spark.sparkContext.textFile("C:\\Users\\KOMAL\\AppData\\Local\\Programs\\Python\\Python310\\Scripts\\example.txt")
    outputrdd =inputrdd.map(lambda x: x.split(","))
    print(outputrdd.collect())

    inputdeptrdd = spark.sparkContext.textFile("C:\\Users\\KOMAL\\AppData\\Local\\Programs\\Python\\Python310\\Scripts\\dept.txt")
    header = inputdeptrdd.first()
    opdeptrdd = inputdeptrdd.filter(lambda line: line != header)
    print(opdeptrdd.collect())

    emprdd = inputrdd.map(lambda x: x.split(",")).map(lambda x: (x[5], x))
    print(emprdd.collect())
    deptrdd = inputdeptrdd.map(lambda x: x.split(",")).map(lambda x: (x[0], x[1]))
    print(deptrdd.collect())

    joinrdd = emprdd.join(deptrdd).cache()
    print(joinrdd.collect())


    def replacefun(element):
        src = element[0]
        src[5] = element[1]
        return src

    print(joinrdd.map(lambda x: x[1]).map(replacefun).collect())

    #joinrdd.saveAsTextFile("C:\\Users\\KOMAL\\PycharmProjects\\pysparkSession\\output\\rddassign1")




