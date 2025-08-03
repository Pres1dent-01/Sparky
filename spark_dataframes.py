# In this Video We will Cover
# PySpark Dataframe
# Reading The Dataset
# Checking the Datatypes of the Column(Schema)
# Selecting Columns And Indexing
# Check Describe option similar to Pandas
# Adding Columns
# Dropping columns
# Renaming Columns

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFramesExample").getOrCreate()
# Method 1:  Reading a CSV file into a DataFrame
# df = spark.read.option("header", "true").csv("test2.csv", inferSchema=True)
# df.show()

# Method 2: Reading a CSV file into a DataFrame with specified schema
df = spark.read.csv("test2.csv", header=True, inferSchema=True)
df.show()

#check schema
df.printSchema()

# Output the type of the DataFrame
print(type(df))

#print columns
print(df.columns)
print(df.head(3))

# Selecting a column
df.select("Name").show()
print(type(df.select("Name")))

# Selecting multiple columns
df.select(["Name", "Age"]).show()

#check datatypes of the columns
print(df.dtypes)

#describe option similar to Pandas
df.describe().show()

#adding a new column
df = df.withColumn("Experience after 2 years", df["Experience"] + 2)
df = df.withColumn("Experience after 3 years", df["Experience after 2 years"] + 1)
df.show()
#Dropping column
# df.drop("Experience after 3 years").show()

#Dropping multiple columns
# df.drop("Experience after 2 years", "Experience after 3 years").show()

#Renaming column
df.withColumnRenamed("Experience", "Experience in years").show()
