import os
import sys

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
#os.environ['JAVA_HOME'] = r'C:\Users\santh\.jdks\corretto-1.8.0_462'
os.environ['JAVA_HOME'] = r'/Users/sannav/Library/Java/JavaVirtualMachines/corretto-1.8.0_472/Contents/Home'

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
###########################################

spark.range(10).show()

print(" ==STARTED== ")

a = 2
print(a)

b = a + 2
print(b)

c = "Zeyobron"
print(c)

d = c + " Analytics"
print(d)
