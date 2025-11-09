import os
import sys

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = r'C:\Users\santh\.jdks\corretto-1.8.0_462'


spark = SparkSession.builder.getOrCreate()
sc= spark.sparkContext
###########################################

#spark.range(10).show()

# print(" ==STARTED== ")
#
# a = 2
# print(a)
#
# b = a + 2
# print(b)
#
# c = "Zeyobron"
# print(c)
#
# d = c + " Analytics"
# print(d)











print("=====started====")

ls  =  [1,2,3,4]
print()
print("======rawList======")
print(ls)

rddls = sc.parallelize(ls)
print()
print("=====rddls======")
print(rddls.collect())

addrdd=rddls.map(  lambda x    :   x   +   2   )
print()
print("====addrdd======")
print(addrdd.collect())






