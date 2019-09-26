import json
from pyspark import SparkContext, SparkConf
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as f
import sys
import time
import multiprocessing

start_time = time.time()

# Run Configurations
input_path1 = sys.argv[1]
input_path2 = sys.argv[2]
output_path1 = sys.argv[3]
output_path2 = sys.argv[4]
# Level of Parallelism - Recommended by Spark
# http://spark.apache.org/docs/latest/tuning.html#level-of-parallelism
cpu_num = multiprocessing.cpu_count()
task_per_cpu = cpu_num * 2

# Spark Configurations
conf = SparkConf().setAppName('HW1 - Task 2').setMaster('local[*]')
sc = SparkContext(conf=conf)

# sc = SparkSession\
#   .builder \
#   .appName("HW1 - Task 2") \
#   .getOrCreate()

# Data Input
distFileR = sc.textFile(input_path1).coalesce(task_per_cpu)
rdd_r = distFileR.map(json.loads)
distFileB = sc.textFile(input_path2).coalesce(task_per_cpu)
rdd_b = distFileB.map(json.loads)

# df_r = sc.read.json(input_path1)
# df_b = sc.read.json(input_path2)

# A. What are the average stars for each state? (DO NOT use the stars information in the business file) (2.5 point)
bid_state = rdd_b.map(lambda s: (s["business_id"], s["state"]))
bid_stars = rdd_r.map(lambda s: (s["business_id"], s["stars"]))
state_stars = bid_state.join(bid_stars)
ss_avg_rdd = state_stars\
  .map(lambda s: (s[1][0], (s[1][1], 1)))\
  .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
  .map(lambda s: (s[0], s[1][0] / s[1][1]))\
  .sortBy(lambda s: (-s[1], s[0]))
# ss_avg_list = ss_avg_rdd.takeOrdered(20, key=lambda s: (-s[1], s[0]))

# state_bid = df_b.select(df_b["state"], df_b["business_id"])
# bid_stars = df_r.select(df_r["business_id"], df_r["stars"])
# state_stars = state_bid.join(bid_stars,
#                              state_bid["business_id"] == bid_stars["business_id"],
#                              how="left")
# ss_avg_df = state_stars.groupBy(state_stars["state"])\
#   .agg(f.avg(state_stars["stars"]).alias("stars"))

# B. You are required to use two ways to print top 5 states with highest stars. You need to compare the time difference
# between two methods and explain the result within 1 or 2 sentences. (3 point)
# Method1: Collect all the data, and then print the first 5 states
start_time1 = time.time()
m1_list = ss_avg_rdd.collect()
print(m1_list[:5])
# m1_list = ss_list_df.sort(f.col("stars").desc(), ss_avg_df["state"]).collect()
# print(m1_list[:5])
duration1 = time.time() - start_time1

# Method2: Take the first 5 states, and then print all
start_time2 = time.time()
m2_list = ss_avg_rdd.take(5)
print(m2_list)
# m2_list = ss_list_df.sort(f.col("stars").desc(), ss_avg_df["state"]).take(5)
# print(m2_list)
duration2 = time.time() - start_time2

# Output Data 1
with open(output_path1, 'w') as op1:
  op1.write("state,stars\n")
  for line in m1_list:
    op1.write(str(line[0]) + "," + str(line[1]) + "\n")

# Output Data 2
answer = dict()
answer["m1"] = duration1
answer["m2"] = duration2
answer["explanation"] = "Method 2 is faster than Method 1, since it only gets the first 5 entities" \
                        " verses getting the entire column in Method 1 which takes more time."
with open(output_path2, 'w') as op2:
  json.dump(answer, op2)

duration = time.time() - start_time
print("Total time - Task 2: ", duration)
