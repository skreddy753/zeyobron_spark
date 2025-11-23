import os
import sys

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = r'C:\Users\santh\.jdks\corretto-1.8.0_462'

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
###########################################

##  STEP   1 ---- READ  FILE

data = sc.textFile("E:\Code\zeyobron_spark\data\dt.txt")
print()
print("======RawData====")
data.foreach(print)

##  STEP   2 ----  SPLIT THE data with Comma

mapsplit = data.map(lambda x: x.split(","))
print()
print("======mapsplit====")
mapsplit.foreach(print)

# Step 3 --- Define Columns Using Named Tuple

from collections import namedtuple

columns = namedtuple('columns', ["id", "tdate", "amt", "category", "product", "mode"])

# Step 4 --- Impose Columns to the data splits

coldata = mapsplit.map(lambda x: columns(x[0], x[1], x[2], x[3], x[4], x[5]))
print()
print("======coldata====")
coldata.foreach(print)

# Step 5 --- Column Filter


prodfil = coldata.filter(lambda x: 'Gymnastics' in x.product)
print()
print("======prodfil====")
prodfil.foreach(print)

df = prodfil.toDF()
df.show()
