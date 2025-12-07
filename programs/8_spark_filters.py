import os
import sys

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
#os.environ['JAVA_HOME'] = r'C:\Users\santh\.jdks\corretto-1.8.0_462'
os.environ['JAVA_HOME'] = r'/Users/sannav/Library/Java/JavaVirtualMachines/corretto-1.8.0_472/Contents/Home'
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
###########################################
# SPARK FILTERS

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

seldf = df.select("txndate", "amount")
seldf.show()

dropdf = df.drop("txndate", "amount")
dropdf.show()

print("========CATEGORY='Exercise'=======")
sincol = df.filter(" category ='Exercise'  ")
sincol.show()

print("========CATEGORY='Exercise' and spendmode='cash'======")
mulcol = df.filter(" category ='Exercise' and spendmode='cash'")
mulcol.show()

print("========CATEGORY='Exercise' or spendmode='cash'======")
mulcolor = df.filter(" category ='Exercise' or spendmode='cash'")
mulcolor.show()

print("========CATEGORY in Exercise and Gymnastics======")
infil = df.filter(" category in ('Gymnastics','Exercise')  ")
infil.show()

print("==subcategory contains Gymnastcs====")
likefil = df.filter("subcategory like '%Gymnastics%'")
likefil.show()

print("==subcategory is null====")
nullfil = df.filter(" subcategory is null ")
nullfil.show()

print("==subcategory is not null====")
notnullfil = df.filter(" subcategory is not null ")
notnullfil.show()

print("==category not equal to Exercise")
notfil = df.filter(" category  != 'Exercise'  ")
notfil.show()
