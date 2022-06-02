from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Context Init") \
                                .config("spark.driver.bindAddress", "localhost") \
                                .config("spark.Ui.port", "4050") \
                                .master("local[*]").getOrCreate()

    # create new samll RDD:
    rdd_small = spark.sparkContext.parallelize([3, 2, 12, 5, 16, 14, 20])
    # print(rdd_small.collect())

# create new big RDD:

    rdd_set = spark.sparkContext.parallelize([[2, 12, 5, 19, 21],
                                              [10, 19, 5, 21, 8],
                                              [34, 21, 14, 8, 10],
                                              [110, 89, 90, 134, 24],
                                              [23, 119, 234, 34, 56]])
    # print(rdd_set.collect())
    #
    # print(rdd_set.first())
    #
    # print(rdd_set.take(2))

    lines = spark.sparkContext.textFile(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\USA.txt")
    print(lines.take(2))

    words = lines.flatMap(lambda x: x.split(' '))
    print(words.collect())

