from pyspark import SparkContext, SparkConf
import sys
import time
import multiprocessing

start_time = time.time()


# Get Data Grouped by user_id or business_id Based on Case Number
def get_grouped_data(ungrouped_data, case_n):
	# User_id case
	if case_n is 1:
		data_p = ungrouped_data.map(lambda s: (s[0], s[1]))
	elif case_n is 2:
		# Business_id case
		data_p = ungrouped_data.map(lambda s: (s[1], s[0]))
	else:
		raise Exception("Case number is incorrect, please check.")

	data_grouped = data_p\
		.map(lambda s: (s[0], [s[1]]))\
		.reduceByKey(lambda x, y: x + y)\
		.values()\
		.persist()

	return data_grouped

# def sort_frequent_items(iterable):
# 	return sorted(iterable)


# Generate Doubleton and Higher Order Pair Permutations
def generate_permutations(iterable, num_set, permutations):
	# # Base case
	# if index >= len(iterable):
	# 	return
	#
	# item_cur = iterable[index]
	# # Pair each item with other item(s)
	# for i in range(index + 1, len(iterable)):
	# 	item_tmp = set(item_cur)
	# 	for item in iterable[i]:
	# 		# No duplication exist from both lists
	# 		if item_tmp.isdisjoint(set(item)):
	# 			item_tmp.add(item)
	# 			if len(item_tmp) == num_set:
	# 				permutations.append(tuple(sorted(item_tmp)))
	# 				item_tmp = set(item_cur)
	# 			else:
	# 				continue
	# 		else:
	# 			# duplication exist, adding the next item in the list
	# 			continue
	# generate_permutations(iterable, index + 1, num_set, permutations)
	for index, item_cur in enumerate(iterable):
		# Pair each item with other item(s)
		for i in range(index + 1, len(iterable)):
			item_tmp = set(item_cur)
			for item in iterable[i]:
				# No duplication exist from both lists
				# if item_tmp.isdisjoint(set(item)):
				item_tmp.add(item)
				if len(item_tmp) == num_set:
					# permutations.append(tuple(sorted(item_tmp)))
					permutations.append(tuple(item_tmp))
					item_tmp = set(item_cur)
				else:
					# duplication exist, adding the next item in the list
					continue


# A-Priori Algorithm
def apriori(iterable, threshold):
	print("Entered A-Priori Algorithm")

	baskets = list(iterable)
	counter = dict()
	candidate_list = list()

	print("Baskets: ", baskets)
	print("Support Threshold: ", threshold)

	# Pass 1
	# Find count of each item from each basket
	for basket in baskets:
		for item in basket:
			item_tuple = tuple([item])
			counter[item_tuple] = 1 if counter.get(item_tuple) is None else counter[item_tuple] + 1
	# Filter out the frequent singles
	# frequent_candidates = sorted(dict({k: v for (k, v) in counter.items() if v >= threshold}))
	frequent_candidates = list(dict({k: v for (k, v) in counter.items() if v >= threshold}).keys())

	# Pass n
	n = 2
	while len(frequent_candidates) > 0:
		# candidate_list.append((ci, 1) for ci in frequent_candidates)
		candidate_list.append(frequent_candidates)
		print("frequent_candidates: ", frequent_candidates)
		print("Candidate_list: ", list(candidate_list))
		permutation_list = list()
		generate_permutations(frequent_candidates, n, permutation_list)
		counter.clear()

		for basket in baskets:
			basket_itemset = set(basket)
			for permutation in permutation_list:
				if basket_itemset.issuperset(permutation):
					counter[permutation] = 1 if counter.get(permutation) is None else counter[permutation] + 1
				else:
					continue

		# frequent_candidates = sorted(dict({k: v for (k, v) in counter.items() if v >= threshold}))
		frequent_candidates = list(dict({k: v for (k, v) in counter.items() if v >= threshold}).keys())
		# print(frequent_items)
		n += 1
	# permutation_list.clear()
	# generate_permutations(frequent_items, 0, 3, permutation_list)
	# print(permutation_list)
	print(candidate_list)

	return candidate_list


# Main Execution
# Run Configurations
case_num = int(sys.argv[1])
support = int(sys.argv[2])
input_path = sys.argv[3]
output_path = sys.argv[4]
# Level of Parallelism - Recommended by Spark
# http://spark.apache.org/docs/latest/tuning.html#level-of-parallelism
cpu_num = multiprocessing.cpu_count()
task_per_cpu = cpu_num * 3

# Spark Configurations
conf = SparkConf().setAppName('HW2 - Task 1').setMaster('local[*]')
sc = SparkContext(conf=conf)

# Data Input
distFile = sc.textFile(input_path).coalesce(task_per_cpu)
rdd = distFile.map(lambda s: s.split(","))
num_part = distFile.getNumPartitions()
support_th = support / num_part

# SON Algorithm
# Divide into market basket model
headers = rdd.first()
data = rdd.filter(lambda s: s != headers)
grouped_data = get_grouped_data(data, case_num)
list_items = data.values().distinct()

# print(grouped_data.collect())

# Pass 1
candidates = grouped_data\
	.mapPartitions(lambda basket: apriori(basket, support_th))\
	.distinct()\
	.collect()
	# .map(lambda s: (s, 1))\
	# .reduceByKey(lambda x, y: 1)\
	# .collect()
	# .map(lambda s: set(s), 1)\
	# .distinct()\
	# .sortByKey(lambda s: s[0])\
	# .collect()
print(candidates)

with open(output_path, 'w') as op:
	op.write("Candidates:" + "\n")
	for item in candidates:
		op.write(str(item).replace(",)", ")").strip("[]") + "\n")
		op.write("\n")


duration = time.time() - start_time
print("Duration: ", duration)
