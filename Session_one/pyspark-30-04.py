import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *




if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]")\
                        .appName("Pyspark-3004")\
                        .getOrCreate()
    print(spark)
    schema = "id int,fname string,lname string,age int,gender string,deptno int,salary long"
    df = spark.read.csv(path=r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input1\employee.csv",
                        schema=schema)
    # df.printSchema()
    # df.show()

    # windowspec = Window.partitionBy("deptno").orderBy(col("salary").desc())

    #cache()---memory and disk
    #persist()--StorageLevel
    df.persist(storageLevel=pyspark.StorageLevel.MEMORY_ONLY)


    #row_number()
    # df.withColumn("row_number",row_number().over(windowspec))\
    #      .select("deptno","salary","row_number").show()


    #rank() and dense_rank()
    # df.select("deptno","salary").withColumn("rank", rank().over(Window.partitionBy("deptno").orderBy(col("salary"))))\
    #     .show()


    #rank() and dense_rank()
    # df.select("deptno","salary").withColumn("rank", dense_rank()
    #                 .over(Window.partitionBy("deptno")
    #                         .orderBy(col("salary")))) \
    #                 .show()

    #lag()
    # df.withColumn("lag_val",lag("salary",1).over(windowspec)).show()

    #lead()
    # df.withColumn("lag_val",lead("salary",1).over(windowspec)).show()

    #aggregate function----avg()
    #df.withColumn("avg",avg("salary").over(Window.partitionBy("deptno"))).show()

    #difference()
    # df.withColumn("lag_val",lead("salary",1).over(windowspec))\
    #     .withColumn("difference", col("lag_val")-col("salary")).show()


    #Spark SQL
    # df.createOrReplaceTempView("employee")
    # spark.sql("select "
    #           "gender,count(*) as count_gender from employee group by gender").show()


#spark UDFs:

    #captital gender(M) return small(m)

    # def lowercasefunc(element):
    #     output = str(element).lower()
    #     return output
    # lowercaseUDF =udf(lambda x: lowercasefunc(x))
    #
    # df.select("gender",lowercaseUDF(col("gender"))).show()



    #gender(M) return fullform Gender(Male)

    # def fullformfunc(element):
    #     if str(element).upper() == "M":
    #         output = "Male"
    #     elif str(element).upper() == "F":
    #         output = "Female"
    #     else:
    #         output = "other"
    #     return output
    #
    # fullformUDF = udf(lambda x: fullformfunc(x))
    #
    # df.select("gender", fullformUDF(col("gender")).alias("gender_new"))\
    #     .drop("gender").show()




    #spark sql UDF
    # spark.udf.register("fullformfunc", fullformfunc, StringType())
    # df.createOrReplaceTempView("employee")
    # spark.sql("select gender, fullformfunc(gender)as new_val from employee").show()




#datetime function:
    from datetime import datetime

    inputdata = [
        (1, "2022-04-01"),
        (2, "2022-04-02"),
        (3, "2022-04-03"),
    ]

    df1 = spark.createDataFrame(inputdata, ["id","input_dt"])
    # df1.printSchema()
    # df1.show()

    #current_date()
    #df1.select(current_date()).show()

    #to_date()
    #df1.select("id", to_date("input_dt")).printSchema()

    #date_format()-------------------Error
    # df1.select("id", date_format("input_dt","DD-MM-yyyy")
    #            .alias("date_format")).show()

    #date difference------------Error
    # df1.select("id","input_dt", datediff(lit(datetime.strptime("2022-04-05","%Y-%m-%d")),
    #            to_date(col("input_dt")))).show()


    # df1.select("id","input_dt",
    #            lit(datetime.strptime("2022-04-05","%y-%m-%d")).alias("new_val")).select(datediff(col("new_val"), to_date(col("input_dt")))).show()
    #
    # -----------------------------------------------Error
    # df1.select("id", "input_dt",
    #            lit(datetime.strptime("2022-04-05", "%y-%m-%d")).alias("new_val")).select(
    #     datediff(col("new_val"), 5)).show()


    # from datetime import datetime
    #
    # inputdata1 = [
    #     (1, datetime.strptime("2022-04-01","yyyy-mm-dd")),
    #     (2, datetime.strptime("2022-04-02","yyyy-mm-dd")),
    #     (3, datetime.strptime("2022-04-03","yyyy-mm-dd")),
    # ]
    #
    # df2 = spark.createDataFrame(inputdata, ["id", "input_dt"])
    # df2.printSchema()



