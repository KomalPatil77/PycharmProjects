from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Context Init") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.Ui.port", "4050") \
        .master("local[*]").getOrCreate()
    # print(spark)



