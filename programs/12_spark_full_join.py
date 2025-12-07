import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = r'C:\Users\santh\.jdks\corretto-1.8.0_462'
# os.environ['JAVA_HOME'] = r'/Users/sannav/Library/Java/JavaVirtualMachines/corretto-1.8.0_472/Contents/Home'
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
###########################################

from pyspark.sql.functions import *

source_rdd = spark.sparkContext.parallelize([

    (1, "A"),
    (2, "B"),
    (3, "C"),
    (4, "D")

], 1)

target_rdd = spark.sparkContext.parallelize([

    (1, "A"),
    (2, "B"),
    (4, "X"),
    (5, "F")

], 2)

# Convert RDDs to DataFrames using toDF()

df1 = source_rdd.toDF(["id", "name"])

df2 = target_rdd.toDF(["id", "name1"])

df1.show()

df2.show()

joindf = df1.join(df2, ["id"], "full")
joindf.show()

commdf = joindf.withColumn("comment", expr("case when name = name1 then 1  else 0 end"))
commdf.show()

fildf = commdf.filter("comment !=1 ")
fildf.show()

procdf = fildf.withColumn("comment", expr("""
                                                    case
                                                    when name1 is null then 'new in source'                                
                                                    when name  is null then  'new  in target'
                                                    else
                                                    'mismatch'
                                                    end                                                
                                                """))

procdf.show()

finaldf = procdf.drop("name", "name1")
finaldf.show()
