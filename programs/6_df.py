import os
import sys

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = r'C:\Users\santh\.jdks\corretto-1.8.0_462'

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
###########################################

csvdf = (

    spark
    .read
    .format("csv")
    .option("header", "true")
    .load("../data/usdata.csv")

)

csvdf.show()

jsondf = (

    spark
    .read
    .format("json")
    .load("../data/file4.json")

)

jsondf.show()

parquetdf = (

    spark
    .read
    .format("parquet")
    .load("../data/file5.parquet")
)
print()
print("======parquetdf======")
parquetdf.show()

parquetdf.createOrReplaceTempView("varanasi")

spark.sql("select  txnno,txndate from  varanasi").show()