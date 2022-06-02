from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import col


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]")\
                            .appName("Dataframe Intro") \
                            .config("spark.driver.bindAddress", "localhost") \
                            .config("spark.Ui.port", "4050") \
                            .getOrCreate()


    df1 = spark.read.csv(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\emp_details1.csv",
                                              inferSchema=True, header=True)
    print(df1.printSchema())
    print(df1.show())

