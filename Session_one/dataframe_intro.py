from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.window import Window as W
from pyspark.sql import types as T
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from operator import itemgetter



if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]")\
                            .appName("Dataframe Intro") \
                            .config("spark.driver.bindAddress", "localhost") \
                            .config("spark.Ui.port", "4050") \
                            .getOrCreate()


    # create a dataframe using existing RDD

    inputdata = [("Ram",100),("Komal",101),("Manoj",102)]
    inputrdd = spark.sparkContext.parallelize(inputdata)
    #print(inputrdd.collect())

    inputdf = inputrdd.toDF(["first_name", "ids"])
    # inputdf.show()
    # inputdf.printSchema()


    inputdf1 = spark.createDataFrame(inputrdd).toDF(*["first_name","ids"])
    # inputdf1.show()
    # inputdf1.printSchema()


    # data = [(1,"Yesh Patil",29,"M"),
    #         (2,"Ram Wagh",30,"M"),
    #         (3,"sita Patil",29,"F")]

    # dataschema = StructType([StructField(name="id", dataType=IntegerType()),
    #                          StructField(name="name", dataType=StringType()),
    #                          StructField("age", dataType=IntegerType()),
    #                          StructField("gender", dataType=StringType())])
    #


    #inputdf2 = spark.createDataFrame(data = data, schema = dataschema)
    # inputdf2.printSchema()
    # inputdf2.show()

    #csv file without header
    csvnoheaderdf = spark.read.csv(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\inputfile_withoutheader.csv")
    # csvnoheaderdf.printSchema()
    # csvnoheaderdf.show()

    # csv file with header
    csvheaderdf = spark.read.csv(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\inputfile_withheader.csv",
                                 inferSchema=True, header=True)
    # csvheaderdf.printSchema()
    # csvheaderdf.show()

    dataschema1 = StructType([StructField(name='id', dataType=IntegerType()),
                              StructField(name='fname', dataType=StringType()),
                              StructField(name='lname', dataType=StringType()),
                              StructField("age", IntegerType()),
                              StructField("gender", StringType()),
                              StructField("deptno", IntegerType()),
                              StructField("Salary", LongType())])

    csvwithschemadf= spark.read.csv(path=r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\inputfile_withheader.csv",
                                     schema=dataschema1, header=True)
    csvwithschemadf.printSchema()
    csvwithschemadf.show()

    #write dataframe in a csv file
    #csvwithschemadf.write.csv(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output\csv_output", sep="\\t")

    #write dataframe in a json file
    #csvwithschemadf.write.json(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output\jsonFileOp", mode="overwrite")
    #csvwithschemadf.write.json(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output\json1_output", lineSep="\\t")

    #write dataframe in a orc file
    #csvwithschemadf.write.orc(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output\orc_output")

    #write dataframe in a parquet file
    #csvwithschemadf.write.parquet(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output\parquet_output")



# create a dataframe from JSON file
    jsonDf = spark.read.json(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output\jsonFileOp\*.json")
    # jsonDf.printSchema()
    # jsonDf.show()

    jsonDf1 = spark.read.json(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output\json1_output\*.json")
    # jsonDf1.printSchema()
    # jsonDf1.show()

    orcDf = spark.read.orc(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output\orc_output")
    # orcDf.printSchema()
    # orcDf.show()

    parquetDf = spark.read.parquet(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output\parquet_output")
    # parquetDf.printSchema()
    # parquetDf.show()

    # create empty dataframe
    rdd1 = spark.sparkContext.parallelize([])
    #print(rdd1.collect())

    #df = spark.createDataFrame(rdd1, schema = dataschema1)\
    #print(df.show())


    # select function
    # csvwithschemadf.printSchema()
    # csvwithschemadf.cache()
    # csvwithschemadf.select(csvwithschemadf.fname).show()
    # csvwithschemadf.select(csvwithschemadf.fname.alias("firstname")).show()
    # csvwithschemadf.select(col("fname"),col("lname")).show()
    # csvwithschemadf.select(["age","gender"]).show()


    #dealing with nested data
    # jsonnesteddf = spark.read.format("json").load(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\nestedfile.json")
    # jsonnesteddf.printSchema()
    # jsonnesteddf.show()
    # jsonnesteddf.select(jsonnesteddf.address.city).show()
    # jsonnesteddf.select(["address.city","address.state"]).show()
    # jsonnesteddf.select("*").show()

    # show column names in dataframe
    #print(csvwithschemadf.columns)

    # withColumns()
    #csvwithschemadf.show()
    # existing column value change
    #csvwithschemadf.withColumn("salary",col("salary")* 10).show()
    # change datatype of existing column
    #csvwithschemadf.withColumn("id", col("id").cast("string")).printSchema()  #Error
    # adding new column
    #csvwithschemadf.withColumn("state", lit(10)).show()  #Error

    # withColumnRenamed
    #csvwithschemadf.withColumnRenamed("fname","firstname").show()


    # filter
    #csvwithschemadf.filter(col("gender")=="M").show()
    #csvwithschemadf.filter((col("gender")=="M") & (col("salary") > 25000)).show()

    # drop(), dropDuplicate(), distinct
    # from pyspark.sql import Row
    # data =[Row(name="Ajay",age=20),
    #        Row(name="Komal",age=27),
    #        Row(name="Ajay",age=20),
    #        Row(name="Ajay",age=30)]
    # df1 = spark.sparkContext.parallelize(data).toDF()
    # #df1.show()
    # df1.printSchema()

    # distinct()
    #df1.distinct().show()

    # dropDuplicate()
    #df1.dropDuplicates(['name']).show()

    # drop()
    #df1.drop('age').show()


    # groupby()----Aggregate functions
    #csvwithschemadf.groupby('deptno').avg('salary').show()


    # # Joins
    # data1 = [Row(deptno=11,deptname='HR'),
    #          Row(deptno=12,deptname='IT'),
    #          ]
    #deptdf = spark.sparkContext.parallelize(data1).toDF()
    # deptdf.printSchema()

    # inner join

    # csvwithschemadf.join(deptdf,
    #                      on=csvwithschemadf.deptno==deptdf.deptno,
    #                      how='inner').select(["id","fname","lname",
    #                                          csvwithschemadf.deptno,"deptname"]).show()

    # left join
    # csvwithschemadf.join(deptdf,
    #                      on=csvwithschemadf.deptno ==deptdf.deptno,
    #                      how = 'left').show()

    # antijoin

    # csvwithschemadf.join(deptdf,
    #                      on=csvwithschemadf.deptno == deptdf.deptno,
    #                      how = 'left_anti').show()


    # union
    # data2 = [Row(deptno=11, deptname='HR'),
    #          Row(deptno=12, deptname='IT'),
    #          Row(deptno=12, deptname='IT'),
    #          ]
    # deptdf1 = spark.sparkContext.parallelize(data2).toDF()
    # deptdf1.printSchema()
    #
    # deptdf.union(deptdf1).show()
    # deptdf.unionAll(deptdf1).distinct().show()


    csvschemadf = spark.read.csv(path=r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\data.csv",
                                 inferSchema=True, header=True)

    csvschemadf.printSchema()
    csvschemadf.show()


    df1 = csvschemadf.withColumn("date",col("date").cast(dataType='date'))\
        .orderBy("id")
    df1.printSchema()
    df1.show()

    df1.select("id","name","price","date").groupBy("id","name").agg(max("date")).show()







































































































































































































































































    orcDf = spark.read.orc(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output\orc_output")
    # orcDf.printSchema()
    # orcDf.show()

    parquetDf = spark.read.parquet(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output\parquet_output")
    # orcDf.printSchema()
    # orcDf.show()






