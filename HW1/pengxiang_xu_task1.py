import json
from pyspark import SparkContext, SparkConf
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as f
import sys
from operator import add
import time
import multiprocessing

start_time = time.time()

# Run Configurations
input_path = sys.argv[1]
output_path = sys.argv[2]
# Level of Parallelism - Recommended by Spark
# http://spark.apache.org/docs/latest/tuning.html#level-of-parallelism
cpu_num = multiprocessing.cpu_count()
task_per_cpu = cpu_num * 2

# Spark Configurations
conf = SparkConf().setAppName('HW1 - Task 1').setMaster('local[*]')
sc = SparkContext(conf=conf)

# sc = SparkSession\
#   .builder \
#   .appName("HW1 - Task 1") \
#   .getOrCreate()

# Data Input
distFile = sc.textFile(input_path).coalesce(task_per_cpu)
rdd = distFile.map(json.loads)\
  .map(lambda s: (s['useful'], s['stars'], s['text'], s['user_id'], s['business_id']))
# df = sc.read.json(input_path)

# A. The number of reviews that people think are useful
# (The value of tag ‘useful’ > 0) (1 point)
# n_review_useful = rdd.map(lambda s: (s['useful'])).filter(lambda s: s > 0).count()
n_review_useful = rdd.filter(lambda s: (s[0] > 0)).count()
# n_review_useful = df.filter(df["useful"] > 0).count()
# n_review_useful = 1

# B. The number of reviews that have 5.0 stars rating (1 point)
# n_review_5_star = rdd.map(lambda s: (s['stars'])).filter(lambda s: s == 5.0).count()
n_review_5_star = rdd.filter(lambda s: (s[1] == 5.0)).count()
# n_review_5_star = df.filter(df["stars"] == 5.0).count()
# n_review_5_star = 2

# C. How many characters are there in the ‘text’ of the longest review (1 point)
n_characters = rdd.map(lambda s: len(s[2])).max()
# n_characters = df.select(f.length("text")).rdd.max()[0]
# n_characters = 3

# D. The number of distinct users who wrote reviews (1 point)
users = rdd.map(lambda s: (s[3]))
n_user = users.distinct().count()
# n_user = df.select(f.countDistinct("user_id")).collect()[0][0]
# n_user = 4

# E. The top 20 users who wrote the largest numbers of reviews and the number of
# reviews they wrote (1 point)
u_user = users.map(lambda s: (s, 1)).reduceByKey(add)
top20_user = u_user.takeOrdered(20, key=lambda s: (-s[1], s[0]))
# top20_user = df.groupBy(df["user_id"])\
#   .agg(f.count(df["user_id"]).alias("cd_user"))\
#   .sort(f.col("cd_user").desc(), df["user_id"].asc())\
#   .take(20)

# F. The number of distinct businesses that have been reviewed (1 point)
businesses = rdd.map(lambda s: (s[4]))
n_business = businesses.distinct().count()
# n_business = df.select(f.countDistinct("business_id")).collect()[0][0]
# n_business = 6

# G. The top 20 businesses that had the largest numbers of reviews and the number
# of reviews they had (1 point)
u_business = businesses.map(lambda s: (s, 1)).reduceByKey(add)
top20_business = u_business.takeOrdered(20, key=lambda s: (-s[1], s[0]))
# top20_business = df.groupBy(df["business_id"])\
#   .agg(f.count(df["business_id"]).alias("cd_business"))\
#   .sort(f.col("cd_business").desc(), df["business_id"].asc())\
#   .take(20)

# Output Data
answer = dict()
answer["n_review_useful"] = n_review_useful
answer["n_review_5_star"] = n_review_5_star
answer["n_characters"] = n_characters
answer["n_user"] = n_user
answer["top20_user"] = top20_user
answer["n_business"] = n_business
answer["top20_business"] = top20_business

with open(output_path, 'w') as of:
  json.dump(answer, of)

duration = time.time() - start_time
print("Total time - task 1: ", duration)
