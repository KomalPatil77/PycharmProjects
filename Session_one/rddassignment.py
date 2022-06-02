from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # create SparkSession
    spark = SparkSession.builder.master("local[*]")\
                                .appName("RDD Assignment") \
                                .config("spark.driver.bindAddress", "localhost") \
                                .config("spark.Ui.port", "4050") \
                                .getOrCreate()

    inputrdd = spark.sparkContext.textFile(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\inputfile.txt")
    inputrdd1 = spark.sparkContext.textFile(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\inputfile1.txt")



    #1. average salary per department

    # deptsalrdd = (inputrdd.map(lambda x: x.split(","))\
    #               .map(lambda x: (x[5], int(x[6])))\
    #               .groupByKey().mapValues(list))\
    #               .map(lambda x: (x[0],sum(x[1])/len(x[1])))
    #print(deptsalrdd.collect())


    #2. provide employee details who has second highest salary
#way-1
    secondHighestSal = min((inputrdd.map(lambda x: x.split(","))
                                    .map(lambda x: int(x[6]))).distinct()\
                                    .takeOrdered(2, key=lambda x: -x))
    empdetailsrdd = (inputrdd.map(lambda x: x.split(","))
                    .filter(lambda x: int(x[6]) == secondHighestSal))
    #print(empdetailsrdd.collect())

#way-2

    emp1 = spark.sparkContext.textFile(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\inputfile.txt")
    emp2 = emp1.sortBy(lambda x: x[6],ascending=False).take(2)
    #print(spark.sparkContext.parallelize(emp2).sortBy(lambda x: x[6],ascending=True).first())

#way-3
    secondHighestSal1 = inputrdd.map(lambda x: x.split(","))\
                                .map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5], int(x[6])))

    #print(inputrdd.collect())
    map = inputrdd.top(2)
    #print(map[1])

# # retrive employee_details with unique records from employee_details1.txt
# # and employee_details1.txt and store in different file

    inputfile = spark.sparkContext.textFile(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\inputfile.txt")
    #print(inputfile.collect())
    inputfile1 = spark.sparkContext.textFile(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\inputfile1.txt")
    #print(inputfile.collect())

    union_rdd = inputfile.union(inputfile1).distinct()
    print(union_rdd.collect())
    #
    # print(union_rdd.saveAsTextFile(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output\union_rdd"))


#






























