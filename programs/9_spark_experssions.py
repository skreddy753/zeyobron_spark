import os
import sys

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['JAVA_HOME'] = r'C:\Users\santh\.jdks\corretto-1.8.0_462'
os.environ['JAVA_HOME'] = r'/Users/sannav/Library/Java/JavaVirtualMachines/corretto-1.8.0_472/Contents/Home'
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
###########################################


# EXPRESSIONS

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
# exprdf = df.selectExpr(
#
#     "cast(txnno as int) as txnno",
#     "txndate",
#     "amount + 1000  as amount",
#     "upper(category) as category",
#     "concat(subcategory,'~zeyo') as subcategory",
#     "spendmode"
#
# )
#
# exprdf.show()

# FULL EXPRESSIONS

data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
]

# Using toDF() to create DataFrame
df = spark.createDataFrame(data).toDF(
    "txnno", "txndate", "amount", "category", "subcategory", "spendmode"
)

df.show()

exprdf = df.selectExpr(

    "cast(txnno as int) as txnno",
    "split(txndate,'-')[2] as year",
    "amount + 1000 as amount",
    "upper(category) as category",
    "concat(subcategory,'~zeyo') as subcategory",
    "spendmode",
    " case when  spendmode='cash' then 1 else 0 end as status "

)
exprdf.show()
