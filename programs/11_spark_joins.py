import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['JAVA_HOME'] = r'C:\Users\santh\.jdks\corretto-1.8.0_462'
os.environ['JAVA_HOME'] = r'/Users/sannav/Library/Java/JavaVirtualMachines/corretto-1.8.0_472/Contents/Home'
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
###########################################

# # INNER JOIN
#
# data4 = [
#     (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani")
# ]
#
# cust = spark.createDataFrame(data4, ["id", "name"])
# cust.show()
#
# data3 = [
#     (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop")
# ]
#
# prod = spark.createDataFrame(data3, ["id", "product"])
# prod.show()
#
# innerj = cust.join(prod, ["id"], "inner")
# innerj.show()


from pyspark.sql.functions import *

# data4 = [
#     (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani")
# ]
#
# cust = spark.createDataFrame(data4, ["id", "name"])
# cust.show()
#
# data3 = [
#     (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop")
# ]
#
# prod = spark.createDataFrame(data3, ["id", "product"])
# prod.show()
#
# print("===========INNER JOIN======")
# innerjoin = cust.join(prod, ["id"], "inner")
# innerjoin.show()
#
# print("===========LEFT JOIN======")
# leftjoin = cust.join(prod, ["id"], "left").orderBy("id")
# leftjoin.show()
#
# print("===========right JOIN======")
# rightjoin = cust.join(prod, ["id"], "right").orderBy("id")
# rightjoin.show()
#
# print("===========Full JOIN======")
# fulljoin = cust.join(prod, ["id"], "full").orderBy("id")
# fulljoin.show()

from pyspark.sql.functions import *

# data4 = [
#     (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani")
# ]
#
# cust = spark.createDataFrame(data4, ["id", "name"])
# cust.show()
#
# data3 = [
#     (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop")
# ]
#
# prod = spark.createDataFrame(data3, ["cid", "product"])
# prod.show()
#
# innerdf = cust.join(prod, cust["id"] == prod["cid"], "inner")
# innerdf.show()
#
# leftdf = cust.join(prod, cust["id"] == prod["cid"], "left")
# leftdf.show()
#
# rightdf = cust.join(prod, cust["id"] == prod["cid"], "right")
# rightdf.show()
#
# fulldf = cust.join(prod, cust["id"] == prod["cid"], "full")
# fulldf.show()
#
# finaldf = (
#
#     fulldf.withColumn("id", expr(" coalesce( id , cid  )   "))
#     .withColumn("replace_id", expr("case when id is null then cid else id end"))
# )
#
# finaldf.show()

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()

leftAnti = cust.join(prod, ["id"], "leftanti")
leftAnti.show()
