from pyspark import SparkContext, SparkConf
import sys
import time
import multiprocessing
from itertools import combinations

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


# Generate Doubleton and Higher Order Pair Permutations
def generate_permutations(iterable, num_set):
	permutations = list()
	min_set_size = 2
	if num_set < min_set_size:
		return permutations
	else:
		for i, item_cur in enumerate(iterable):
			item_cur_set = set(item_cur)
			# Pair each item with other item(s)
			for j in range(i + 1, len(iterable)):
				item_next = iterable[j]
				# All permutations of pairs of two - order 1's
				if num_set == min_set_size:
					permutations.append(item_cur + item_next)
				else:
					# Higher order pairs
					# Adding new permutation of two frequent itemsets
					if item_cur[0: num_set - min_set_size] == item_next[0: num_set - min_set_size]:
						permutations.append(tuple(sorted(item_cur_set.union(item_next))))
					else:
						# Eliminate the items that is not shown in frequent itemset from the last filtering pass
						break
	return permutations


def get_frequent_candidates(baskets, threshold, permutation_list=None, num_set=None):
	counter = dict()
	for basket in baskets:
		if num_set is None or num_set == 1:
			# Find count of each item from each basket
			for item in basket:
				item_tuple = tuple([item])
				counter[item_tuple] = 1 if counter.get(item_tuple) is None else counter[item_tuple] + 1
		elif num_set > 1:
			basket_itemset = set(basket)
			# Find count of each itemset from each basket
			for permutation in permutation_list:
				if basket_itemset.issuperset(permutation):
					counter[permutation] = 1 if counter.get(permutation) is None else counter[permutation] + 1
				else:
					continue
		else:
			return list()
	frequent_candidates = sorted(dict({k: v for (k, v) in counter.items() if v >= threshold}))
	return frequent_candidates


# A-Priori Algorithm
def apriori(iterable, count, threshold):
	baskets = list(iterable)
	support_th = threshold * (len(baskets) / count)
	candidate_list = list()
	# print("Support Threshold: ", support_th)
	# print("len basket: ", len(baskets))
	# print(baskets)

	# Pass 1
	frequent_candidates = get_frequent_candidates(baskets, support_th)

	# Pass n
	n = 2
	while len(frequent_candidates) > 0:
		candidate_list.extend(frequent_candidates)
		# print("frequent_candidates: ", frequent_candidates)
		# print("candidate_list: ", candidate_list)
		permutation_list = generate_permutations(frequent_candidates, n)
		# print("permutation_list: ", permutation_list)
		frequent_candidates = get_frequent_candidates(baskets, support_th, permutation_list, n)
		n += 1

	return candidate_list


def count_freq(iterable, candidates):
	baskets = list(iterable)
	freq_items = list()

	# Calculate the count of each candidate in basket
	for candidate in candidates:
		count = 0
		c_set = set(candidate)
		for basket in baskets:
			if c_set.issubset(basket):
				count += 1
		freq_items.append((candidate, count))

	return freq_items


def record_data(dataset, output_file):
	last_len = 0
	# count = 0
	for item in dataset:
		# Group the same length record together
		item_size = len(tuple(item))
		if item_size > last_len:
			output_file.write("" if last_len == 0 else "\n\n")
			last_len = item_size
		else:
			output_file.write(",")

		# Candidate
		record = str(item) \
			.replace(",)", ")") \
			.strip("[]")
		output_file.write(record)
		# count += 1
	# print(count)


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
distFile = sc.textFile(input_path, minPartitions=2).coalesce(task_per_cpu)
rdd = distFile.map(lambda s: s.split(","))

# SON Algorithm
# Divide into market basket model
headers = rdd.first()
data = rdd.filter(lambda s: s != headers)
grouped_data = get_grouped_data(data, case_num)

num_count = grouped_data.count()
num_part = grouped_data.getNumPartitions()

# Pass 1
candidates = grouped_data\
	.mapPartitions(lambda basket: apriori(basket, num_count, support))\
	.map(lambda s: (s, 1))\
	.reduceByKey(lambda x, y: 1)\
	.keys()\
	.collect()
candidates_sorted = sorted(candidates, key=lambda k: (len(k), k))

# Pass 2
freq_itemsets = grouped_data\
	.mapPartitions(lambda basket: count_freq(basket, candidates))\
	.reduceByKey(lambda x, y: (x + y))\
	.filter(lambda s: s[1] >= support)\
	.map(lambda s: s[0])\
	.collect()
freq_itemsets_sorted = sorted(freq_itemsets, key=lambda k: (len(k), k))

# Data Output
with open(output_path, 'w') as op:
	# Add Candidates
	op.write("Candidates:" + "\n")
	record_data(candidates_sorted, op)
	op.write("\n\n")

	# Add Frequent Itemsets
	op.write("Frequent Itemsets:" + "\n")
	record_data(freq_itemsets_sorted, op)

# Testing tool
# with open("./grouped_data.txt", 'w') as gf:
# 	grouped_data_list = grouped_data.collect()
# 	list_items = data.values().distinct().collect()
#
# 	# Get grouped data
# 	for line in grouped_data_list:
# 		gf.write(str(line) + "\n")
#
# 	# Convert each line of grouped data to set
# 	group_data_set = list()
# 	for line in grouped_data_list:
# 		group_data_set.append(set(tuple(line)))
# 	# print(group_data_set)
#
# 	# Create all combinations
# 	list_data_set = dict()
# 	for i in range(len(list_items)):
# 		if i < 1:
# 			continue
# 		else:
# 			list_comb = combinations(list_items, i)
# 			for comb in list_comb:
# 				list_data_set[tuple(sorted(list(comb)))] = 0
# 	# print(list_data_set)
#
# 	# Check results
# 	for dict_p in list_data_set:
# 		key_set = set(dict_p)
# 		for line_set in group_data_set:
# 			if key_set.issubset(line_set):
# 				list_data_set[dict_p] += 1
#
# 	for data_p in sorted(list_data_set, key=lambda k: (len(k), k)):
# 		if list_data_set[data_p] >= support:
# 			gf.write(str(data_p) + ": " + str(list_data_set[data_p]) + "\n")


duration = time.time() - start_time
print("Duration: ", duration)
