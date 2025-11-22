import os
import sys

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = r'C:\Users\santh\.jdks\corretto-1.8.0_462'

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
###########################################

print(sc.parallelize(["first", "second"]).collect())

import time

time.sleep(360)  # in browser hit this url ---->   localhost:4040

# localhost:4040
