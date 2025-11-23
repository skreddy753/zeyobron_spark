import os
import sys

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = r'C:\Users\santh\.jdks\corretto-1.8.0_462'

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
###########################################

# SQL GET READY CODE
data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
# df.show()

data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
# df1.show()

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

cust = spark.createDataFrame(data4, ["id", "name"])
# cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
# prod.show()

df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")

##########################################

# print("### Select required columns ###")
# spark.sql("select id, tdate from df").show()
#
# print("### Filter data ###")
# spark.sql("select * from df where category = 'Exercise'").show()
#
# spark.sql("select id,tdate,category,spendby from df where category = 'Exercise' and spendby = 'cash'").show()
#
# print("### Multi value filter data ###")
# spark.sql("select * from df where category in ('Exercise', 'Gymnastics')")

# print("### contains data ###")
# spark.sql("select * from df where product like ('%Gymnastics%')").show()

# print("### not equals ###")
# spark.sql("select * from df where category != 'Exercise'").show()
#
# spark.sql("select * from df where category not in ('Exercise')").show()
#
# spark.sql("select * from df where category != 'Exercise' and category != 'Gymnastics'").show()
#
# spark.sql("select * from df where category not in ('Exercise', 'Gymnastics')").show()

# print("### Filter null values ###")
# spark.sql("select * from df where product is null").show()
#
# spark.sql("select * from df where product is not null").show()

# print("### max ###")
# spark.sql("select max(id) as maxId from df").show()
#
# print("### min ###")
# spark.sql("select min(id) as minId from df").show()

# print("### count ###")
# spark.sql("select count(1) from df").show() # high performance
#
# spark.sql("select count(*) from df").show()

# print("### Conditional Statement ###")
# spark.sql("select *, case when spendby = 'cash' then 1 else 0 end as status from df").show()
#
# spark.sql("select *, case when spendby = 'cash' then 1 "
#                          "when spendby = 'paytm' then 'NA' "
#                          "else 0 end as status from df").show()

# print("### concat ###")
# spark.sql("select *, concat(id,'-',category) as concatData from df").show()
#
# spark.sql("select *, concat_ws('_', id,category) as concatData from df").show()

# print("### case convertion ###")
# spark.sql("select category, upper(product) from df").show()
#
# spark.sql("select category, lower(product) from df").show()

# print("### CEIL & Round ###") # CEIL - rounding off to upper value
# spark.sql("select amount, ceil(amount), round(amount) from df").show()

# print("### Replace nulls ###")
# spark.sql("select product, coalesce(product, 'NA') as nullrep from df").show()

# print("### trim spaces ###")
# spark.sql("select trim(product) from df").show()

# print("### distinct ###")
# spark.sql("select distinct category from df").show()
#
# spark.sql("select distinct category,spendby from df").show()

# print("### substring ###")
# spark.sql("select substring(product,1,10) as subStringData from df").show()

# print("### split ###")
# spark.sql("select product, split(product,' ')[0] as splitData from df").show()

# print("### union all ###")
# spark.sql("select * from df union all select * from df1").show()
#
# spark.sql("select * from df union select * from df1").show() # remove duplicates

print("### Aggregate ###")
# spark.sql("select category, sum(amount) as totalAmount from df group by category").show()
#
# spark.sql("select category, sum(amount) as totalAmount from df group by category,spendby").show()

# spark.sql("select category, sum(amount) as totalAmount, count(amount) as countData from df group by category,spendby").show()
#
# spark.sql("select category, sum(amount) as totalAmount, count(*) as countData from df group by category,spendby").show()

spark.sql("select category, max(amount) as maxAmount from df group by category")