import os
import sys

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = r'C:\Users\santh\.jdks\corretto-1.8.0_462'

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
###########################################

# csvdf = (
#
#     spark
#     .read
#     .format("csv")
#     .option("header", "true")
#     .load("../data/usdata.csv")
#
# )
#
# csvdf.show()
#
# jsondf = (
#
#     spark
#     .read
#     .format("json")
#     .load("../data/file4.json")
#
# )
#
# jsondf.show()
#
# parquetdf = (
#
#     spark
#     .read
#     .format("parquet")
#     .load("../data/file5.parquet")
# )
# print()
# print("======parquetdf======")
# parquetdf.show()
#
# parquetdf.createOrReplaceTempView("varanasi")
#
# spark.sql("select  txnno,txndate from  varanasi").show()

# DATAFRAME READ AND FORMULA

# csvdf = (
#
#     spark
#     .read
#     .format("csv")
#     .option("header", "true")
#     .load("../data/usdata.csv")
#
# )
#
# csvdf.show()
#
# csvdf.createOrReplaceTempView("rishab")
#
# spark.sql("select * from rishab where state='LA'").show()

## select and Drop

data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None , "cash")
]

# Using toDF() to create DataFrame
df = spark.createDataFrame(data).toDF(
    "txnno", "txndate", "amount", "category", "subcategory", "spendmode"
)

df.show()


### SELECT columns

seldf = df.select("txndate","amount")
seldf.show()

### DROP columns

dropdf = df.drop("txndate","amount")
dropdf.show()





# data = [
#     ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
#     ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
#     ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
#     ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
#     ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
#     ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
# ]
#
# # Using toDF() to create DataFrame
# df = spark.createDataFrame(data).toDF(
#     "txnno", "txndate", "amount", "category", "subcategory", "spendmode"
# )
#
# df.show()
#
# print("===========CATEGORY = EXERCISE======")
# sincol = df.filter(" category ='Exercise' ")
# sincol.show()
#
# print("===========CATEGORY = EXERCISE and spendby=cash  ======")
# mulcol = df.filter("   category ='Exercise'  and spendmode='cash'    ")
# mulcol.show()
#
# print("===========CATEGORY = EXERCISE or  spendby=cash  ======")
# mulcolor = df.filter("   category ='Exercise'  or spendmode='cash'    ")
# mulcolor.show()
#
# print("===========CATEGORY = EXERCISE & GYMNASTICS  ======")
# infil = df.filter("   category  in  ( 'Exercise' ,  'Gymnastics' )    ")
# infil.show()
