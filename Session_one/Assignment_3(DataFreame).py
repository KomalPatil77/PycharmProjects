from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql import Window
from pyspark.sql.functions import *




if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]")\
                                 .appName("Create DataFrame") \
                                 .config("spark.driver.bindAddress", "localhost") \
                                 .config("spark.Ui.port", "4050") \
                                 .getOrCreate()


 #Q.1 Create DataFrame and write a dataframe values using csv,json,orc,parquet file format
 # and try this with various ways:

     #1. Create DataFrame from a list of Row

    # dept = [Row("Finance",10),
    #          Row("Marketing",20),
    #          Row("Sales",30),
    #          Row("IT",40)]
    #
    # deptColumns = ["dept_name","dept_id"]
    # Deptdf = spark.createDataFrame(data = dept, schema = deptColumns)#     # deptDF.printSchema()
    # Deptdf.show()

    #add column using schema

    # deptSchema = StructType([StructField('firstname', StringType(), True),
    #                          StructField('middlename', StringType(), True),
    #                          StructField('lastname', StringType(), True)])
    #
    # deptDF1 = spark.createDataFrame(data=dept, schema=deptSchema)
    # deptDF1.printSchema()
    # deptDF1.show()



#2. Create DataFrame using existing RDD

    # inputdata = [("UttarPradesh", 122000, 89600, 12238),
    #              ("Maharashtra", 454000, 380000, 67985),
    #              ("TamilNadu", 115000, 102000, 13933),
    #             ("Karnataka", 147000, 111000, 15306),
    #             ("Kerala", 153000, 124000, 5259)]
    #
    # rdd1 = spark.sparkContext.parallelize(inputdata)
    #print(rdd1.collect())

    #Dataframe = spark.createDataFrame(rdd1, schema = ["State","Cases","Recovered","Deaths"])
    # print(Dataframe.printSchema())
    # print(Dataframe.show())


    #read & write dateframe in csv file

    # dataschema2 = StructType([StructField(name="state", dataType=StringType()),
    #                            StructField(name="cases", dataType=IntegerType()),
    #                            StructField(name="recovered", dataType=LongType()),
    #                            StructField(name="deaths", dataType=LongType())
    #                           ])



    # create a dataframe from CSV file

    #Read CSV file
    #csvdf= spark.read.csv(path=r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input1\csv_file.csv",
                           #inferSchema=True, header=True)
    # print(csvdf.printSchema())
    # print(csvdf.show())

    #Write CSV file
    #csvdf.write.csv(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output1\csvoutput1", sep="\\t")


    # create a dataframe from JSON file

    #write JSON file
    #csvdf.write.json(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output1\jsonoutput", mode="overwrite")

    #Read JSON file
    #jsondf = spark.read.json(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output1\jsonoutput\*")
    # print(jsondf.printSchema())
    # print(jsondf.show())



    #create a dataframe from ORC file

    #Write ORC file
    #csvdf.write.orc(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output1\orcoutput")

    #Read ORC file
    #orcdf = spark.read.orc(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output1\orcoutput")
    # print(orcdf.printSchema())
    # print(orcdf.show())


    #csvdf.withColumnRenamed("Cases","xyz").show()
    # create a dataframe from parquet file

    #Write parquet file
    #csvdf.write.parquet(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\output1\parquetoutput")    # Error




#Q.2 Apply select() transformation on dataframe using various ways
    # data = [("James","Smith","USA","CA"),
    #         ("Michael","Rose","USA","NY"),
    #         ("Robert","Williams","USA","CA"),
    #         ("Maria","Jones","USA","FL")]


    # inputrdd1 = spark.sparkContext.parallelize(data)
    # print(inputrdd1.collect())

    # df1 = inputrdd1.toDF(["Firstname","Lastname","Country","State"])
    # print(df1.show())


    #1. Select single or multiple columns from DataFrame
    #df1.select(df1.Country).show()
    #df1.select("Firstname","Lastname").show()

    #2. Using Dataframe object name
    #df1.select(df1("Firstname"),df1("Lastname")).show()

    #3. Using col function, use alias() to get alias name
    #df1.select(col("Firstname").alias("fname"),col("Lastname")).show()

    #4. Show all columns from DataFrame
    #df1.select("*").show()


    #5. Show columns by regular expression
    #df1.select(df1.colRegex("`^.*name*`")).show()



 # Using Nested_data select() transformation on dataframe

    # nested_data = [(("James","Mike","Smith"),"OH","M",2000),
    #                         (("Anna","Rose","Brown"),"NY","F",3000),
    #                          (("Julia","Jen","Williams"),"OH","F",4000),
    #                         (("Maria","Anne","Jones"),"NY","M",1500),
    #                          (("Jen","Mary","Brown"),"NY","M",5000),
    #                          (("Mike","Mary","Williams"),"OH","M",7000)
    #                          ]

    # schema = StructType([StructField('name',StructType([
    #                                  StructField('firstname', StringType()),
    #                                  StructField('middlename', StringType()),
    #                                  StructField('lastname', StringType()),
    #                                  ])),
    #                                 StructField('state', StringType()),
    #                                 StructField('gender', StringType()),
    #                                  StructField('salary', IntegerType()),
    #                                  ])
    #
    # df2 = spark.createDataFrame(data = nested_data, schema = schema)
    # df2.printSchema()
    # df2.show()
    #Return column name
    #df2.select("name").show()

    # select the specific column from a nested struct

    #df2.select("name.firstname","name.lastname").show()

    # get all columns from struct column.
    #df2.select("name.*").show()
    # Show the summary of the DataFrame
    #df2.select("state","gender").describe().show()





#Q.3 Apply withColumn() transformation on dataframe using various ways

    # Add a New Column to DataFrame
    #df2.withColumn("Country", lit("USA")).show()


    # Add multiple columns at a time
    # df2.withColumn("Dept_name", lit("IT"))\
    #     .withColumn("join_date", lit(2010)).show()


    # Change Value of an Existing Column
    #df2.withColumn("Salary", col("Salary")+10).show()

    #Derive New Column From an Existing Column
    #df2.withColumn("CopiedColumn", col("Salary")* -1).show()

    # Change Column Data Type
    #df2.withColumn("salary", col("salary").cast("String")).printSchema().show()  #Error

    #Rename Column Name
    #df2.withColumnRenamed("gender","sex").show()

    # Drop column
    #df2.drop("salary").show()


    # filters the name in Data Frame.
    # df2.filter(df2.state == "NY").show()
    # df2.filter(col("gender") == "M").show()

    #DataFrame filter() with SQL Expression
    # df2.filter("gender == 'M'").show()
    # df2.where("gender == 'F'").show()

    #Filter Operation over multiple columns.using OR orerator
    #df2.filter("state = 'OH' or gender = 'F'").show()

    #Using AND operator : filter data only when both the condition are True.
    #df2.filter("state = 'NY' and gender = 'M'").show()

    # Filter IS IN List values
    #list =["OH","CA","DE"]
    # df2.filter(df2.state.isin(list)).show()

    # Filter Based on Starts With, Ends With, Contains

    #Using startswith
    #df2.filter(df2.state.startswith("N")).show()

    # using endswith
    #df2.filter(df2.state.endwith("H")).show()

    #contains
    #df2.filter(df2.state.contains("H")).show()




# drop(), dropDuplicate(), distinct

    # inputdata = [("James", "Sales", 3000),
    #             ("Michael", "Sales", 4600),
    #             ("Robert", "Sales", 4100),
    #             ("Maria", "Finance", 3000),
    #             ("James", "Sales", 3000),
    #             ("Scott", "Finance", 3300),
    #             ("Jen", "Finance", 3900),
    #             ("Jeff", "Marketing", 3000),
    #             ("Kumar", "Marketing", 2000),
    #             ("Saif", "Sales", 4100)]

    # columns = ["employee_name", "department", "salary"]
    # inputdf = spark.createDataFrame(data = inputdata, schema = columns)
    # inputdf.printSchema()
    # inputdf.show()

    #Get Distinct Rows
    #distinctDF = inputdf.distinct()
    # print("Distinct count: " + str(distinctDF.count()))
    # distinctDF.show()


    #Drop duplicates
    #df1 = inputdf.dropDuplicates()
    # print("Distinct count: " + str(df1.count()))
    # df1.show()



    #Distinct of Selected Multiple Columns
    #dropDisDF = inputdf.dropDuplicates(["department","salary"])
    # print("Distinct count of department & salary : "+str(dropDisDF.count()))

    # dropDisDF.show()

    # Drop column
    # inputdf.drop("department").show()


#Spark SQl Aggregate Function

    #avg()
    #inputdf.groupby('department').avg('salary').show()
    #inputdf.groupby('department').mean('salary').show()

    #max()
    #inputdf.groupby('department').max('salary').show()
    #inputdf.select(max('salary')).show()

    #min()
    #inputdf.groupby('department').min('salary').show()
    #inputdf.select(min('salary')).show()

    #sum()
    #inputdf.groupby('department').sum('salary').show()


# Joins

    # emp = [(1, "Smith", -1, "2018", "10", "M", 3000),
    #        (2, "Rose", 1, "2010", "20", "M", 4000),
    #        (3, "Williams", 1, "2010", "10", "M", 1000),
    #        (4, "Jones", 2, "2005", "10", "F", 2000),
    #        (5, "Brown", 2, "2010", "40", "", -1),
    #        (6, "Brown", 2, "2010", "50", "", -1)]
    #
    # empColumns = ["emp_id", "name", "superior_emp_id", "year_joined",
    #               "emp_dept_id", "gender", "salary"]
    #
    # empDF = spark.createDataFrame(data=emp, schema=empColumns)
    # empDF.printSchema()
    # empDF.show()

    # dept = [("Finance", 10),
    #         ("Marketing", 20),
    #         ("Sales", 30),
    #         ("IT", 40)]
    #
    # deptColumns = ["dept_name","dept_id"]
    # deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
    # deptDF.printSchema()
    # deptDF.show()


#Inner Join DataFrame
    # empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "inner")\
    #     .show()


#Left Outer Join
    # empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftouter")\
    #         .show()


#Right Outer Join

    # empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "right") \
    #     .show()
    # empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "rightouter") \
    #     .show()

#Full Outer Join

    # empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "outer") \
    #     .show()
    # empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "full") \
    #     .show()
    # empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "fullouter") \
    #     .show()

#Left Semi Join

    # empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftsemi") \
    #     .show()

#Left Anti Join

    # empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftanti") \
    #     .show()






    #rank() and dense_rank()
    # df.withColumn("rank",rank().over(window.partitionBy("deptno")
    #                                  .orderBy(col("salary")))).show()
# print(spark)
#



    #Retrive record which is having maximum date with respective of id, name column
    #ERROR

    # inputData = [(1,"xyz",200,2021-1-1),
    #             (2,"pqr",201,2021-1-1),
    #             (2,"pqr",202,2021-1-2),
    #             (2,"pqr",203,2021-1-3),
    #             (2,"pqr",204,2021-1-4),
    #             (3,"mnp",205,2021-1-1),
    #             (3,"mnp",206,2021-1-2),
    #             (3,"mnp",207,2021-1-3),
    #             ]
    #
    # df = spark.createDataFrame(inputData, ["id", "name", "price", "date"])
    # df.createOrReplaceTempView("stats")
    # spark.sql(
    #     "select id,name,price,date from (select id,name,price,date,\
    #     row_number() over(partition by id,name order by date desc),\
    #     rownum from stats_ where rownum=1"
    # ).show()


    #ERROR
    # Data = [(1,"xyz",200,2021-1-1),
    #             (2,"pqr",201,2021-1-1),
    #             (2,"pqr",202,2021-1-2),
    #             (2,"pqr",203,2021-1-3),
    #             (2,"pqr",204,2021-1-4),
    #             (3,"mnp",205,2021-1-1),
    #             (3,"mnp",206,2021-1-3),
    #             (3,"mnp",207,2021-1-6),
    #             ]

    #df5 = spark.createDataFrame(Data, ["id","name","price","date"])
    # df5.printSchema()
    # df5.show()
    df5 = spark.read.csv(r"C:\Users\KOMAL\PycharmProjects\pysparkSession\input\data.csv",
                         inferSchema=True, header=True)
    #df5.show()

#way-1
    # df5.select("*").groupBy("id","name").agg(max("date")).show()
    # df5.select("*").groupBy("id","name").avg("price").show()

#way-2
    windowSpecAgg = Window.partitionBy(["id","name"]).orderBy(col("date").desc())
    # df5.withColumn("Max",max(col("date")).over(windowSpecAgg)).show()
    #df5.select("id","name","price","date",max("date").over(windowSpecAgg)).show()
    df5.select("id","name","price","date", row_number().over(windowSpecAgg)\
        .alias("rownum")).filter(col("rownum")==1).show()







    # table = [(1,"xyz",200,(2021-1-1),
    #         (2,"pqr",201,2021-1-1),
    #         (2,"pqr",202,2021-1-2),
    #         (2,"pqr",203,2021-1-3),
    #         (2,"pqr",204,2021-1-4),
    #         (3,"mnp",205,2021-1-1),
    #         (3,"mnp",206,2021-1-3),
    #         (3,"mnp",207,2021-1-6)]


    #2. Expected output as below
    windowSpec =Window.partitionBy("id").orderBy("id")

    df6 = df5.withColumn("To date",df5.lead("date").over(windowSpec))
    df6.na.fill("2999-12-31",subset=["To date"]).show()


