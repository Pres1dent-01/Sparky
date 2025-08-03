# Dropping Columns
# Dropping Rows
# Various Parameter In Dropping functionalities
# Handling Missing values by Mean, MEdian And Mode

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HandlingMissingValues").getOrCreate()

df = spark.read.csv("test2.csv", header=True, inferSchema=True)
df.show()

#Droping columns with any missing values
df.na.drop().show() # This will drop any row that has at least one null value

df.na.drop(how="any").show()  # Equivalent to the above, drops rows with any null values
df.na.drop(how="all").show()  # Drops rows only if all values are
df.na.drop(how="any", thresh=2).show() #minimum number of non-null values required to keep the row

# Dropping rows with missing values in specific columns
df.na.drop(subset=["Experience"]).show()  # Drops rows where Experience is null

#Filling missing values
df.na.fill("Missing Value").show()  # Fills all null values with "Missing Value"
df.na.fill("Missing Values", "Experience").show()  #Fills null values in the "Experience" column with "Missing Values"

# Filling with specific values for each column
from pyspark.ml.feature import Imputer

imputer = Imputer(
    inputCols=['age', 'Experience', 'Salary'], 
    outputCols=["{}_imputed".format(c) for c in ['age', 'Experience', 'Salary']]
    ).setStrategy("median")

# Add imputation cols to df
imputer.fit(df).transform(df).show()