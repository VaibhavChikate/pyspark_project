'''
data look like this--
101,vaibhav,mech
102,sagar,cs
103,sampada,ee
'''

from pyspark import *
sc=SparkContext("local","myapp")
rdd=sc.textFile("C:/Users/BlackPerl/OneDrive/Desktop/AWS & Spark/b.csv")
a=rdd.collect()
print(a)
map_rdd=rdd.map(lambda x:x.split(","))
print(map_rdd.collect())
print(map_rdd.count())
flatmap_rdd=rdd.flatMap(lambda x:x.split(","))
print(flatmap_rdd.collect())
print(flatmap_rdd.count())

#flatmap just spread every string if it is delimited by ","(because we mention "," in split() method)
print("second rdd")
list=["vaibhav","sagar","umesh","mangesh"]
z=sc.parallelize(list)
print(z.collect())
m=z.map(lambda x:x.split(","))
print(m.count())
f=z.flatMap(lambda x:x.split(","))
print(f.count())

