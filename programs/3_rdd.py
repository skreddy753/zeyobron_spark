import os
import sys

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = r'C:\Users\santh\.jdks\corretto-1.8.0_462'

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
###########################################

# print("=====started====")

# ls  =  [1,2,3,4]
# print()
# print("======rawList======")
# print(ls)
#
# rddls = sc.parallelize(ls)
# print()
# print("=====rddls======")
# print(rddls.collect())
#
# addrdd=rddls.map(  lambda x    :   x   +   2   )
# print()
# print("====addrdd======")
# print(addrdd.collect())
#
# mulrdd = rddls.map( lambda x : x * 10 )
# print()
# print("====mulrdd======")
# print(mulrdd.collect())
#
# divrdd = rddls.map( lambda x : x / 2 )
# print()
# print("====divrdd======")
# print(divrdd.collect())
#
# fillrdd = rddls.filter(lambda x : x > 2)
# print()
# print("====fillrdd======")
# print(fillrdd.collect())


# listr = ["zeyobron", "zeyo", "tera"]
#
# rddstr = sc.parallelize(listr)
#
# addstr = rddstr.map(lambda s : s + " Analytics")
# print()
# print("====addstr====")
# print(addstr.collect())
#
# repstr = rddstr.map(lambda s : s.replace("zeyo", ""))
# print()
# print("====repstr====")
# print(repstr.collect())
#
# filstr = rddstr.filter(lambda s : "zeyo" in s)
# print()
# print("====filstr====")
# print(filstr.collect())


# FLATMAP


# lis = ["A~B", "C~D"]
# print()
# print("=============lis============")
# print(lis)
#
# rdds = sc.parallelize(lis)
# print()
# print("=============rdds============")
# print(rdds.collect())
#
# flatrdd = rdds.flatMap(lambda x: x.split("~"))
# print()
# print("=============flatrdd============")
# print(flatrdd.collect())


# rawList = ["state->TN~city->Chennai", "state->kerala~city->Trivandrum"]
# print()
# print("===rawlist===")
# print(rawList)
#
# rdd = sc.parallelize(rawList)
# print()
# print("=====rdd=====")
# print(rdd.collect())
#
# data = rdd.flatMap(lambda x: x.split("~"))
# print()
# print("=====data=====")
# print(data.collect())
#
# states = data.filter(lambda x: "state" in x).map(lambda x: x.replace("state->", ""))
# print()
# print("=====states=====")
# print(states.collect())
#
# cities = data.filter(lambda x: "city" in x).map(lambda x: x.replace("city->", ""))
# print()
# print("=====cities=====")
# print(cities.collect())
#


#print("======STARTED======")

# filerdd = sc.textFile("E:\Code\zeyobron_spark\data\state.txt")
# print()
# print("======filerdd=======")
# filerdd.foreach(print)  # COLAB -----   print(filerdd.collect())
#
# flatrdd1 = filerdd.flatMap(lambda x: x.split("~"))
# print()
# print("======flatrdd1=======")
# flatrdd1.foreach(print)  # COLAB -----   print(flatrdd1.collect())
#
# statelis1 = flatrdd1.filter(lambda x: 'State' in x)
# print()
# print("======statelis=======")
# statelis1.foreach(print)  # COLAB -----   print(statelis1.collect())
#
# states1 = statelis1.map(lambda x: x.replace("State->", ""))
# print()
# print("======states=======")
# states1.foreach(print)  # COLAB -----   print(states1.collect())
#
# citylis1 = flatrdd1.filter(lambda x: 'City' in x)
# print()
# print("======citylis=======")
# citylis1.foreach(print)  # COLAB -----   print(citylis1.collect())
#
# repcity1 = citylis1.map(lambda x: x.replace("City->", ""))
# print()
# print("======repcity=======")
# repcity1.foreach(print)  # COLAB -----   print(repcity1.collect())



print("======STARTED======")

usdata = sc.textFile("E:/Code/zeyobron_spark/data/usdata.csv")
print()
print("====usdata=====")
usdata.foreach(print)

lendata = usdata.filter(lambda x: len(x) > 200)
print()
print("====lendata=====")
lendata.foreach(print)

flatdata = lendata.flatMap(lambda x: x.split(","))
print()
print("====flatdata=====")
flatdata.foreach(print)

repdata = flatdata.map(lambda x: x.replace("-", ""))
print()
print("====repdata=====")
repdata.foreach(print)

adddata = repdata.map(lambda x: x + " ,zeyo")
print()
print("====adddata=====")
adddata.foreach(print)

# adddata.saveAsTextFile("file:///D:/rddout")
# print("===Save Completed===")