from pyspark import SparkContext, SparkConf
import sys
import time
import multiprocessing
from itertools import combinations
import random
from math import sqrt


# Generate a list of primes selected from 2 to 1000
def gen_primes(list_len):
	prime_list = [i for i in range(2, 1000) if is_prime(i)]
	return random.choices(prime_list, k=list_len)


# Determine if a number is a prime number
def is_prime(x):
	if x % 2 == 0:
		return True
	elif any(x % i == 0 for i in range(3, int(sqrt(x)) + 1, 2)):
		return False
	else:
		return True


# Determine band and row number
def choose_band_row(num_hash_func):
	last_b = 1
	last_r = 1
	s_max = 0
	b_max = 0
	for b in range(num_hash_func - 1, 1, -1):
		if num_hash_func % b == 0:
			r = num_hash_func / b
			# Threshold - ideally a little less than 50%
			s = (1 / b) ** (1 / r)
			if s >= 0.5:
				return int(last_b), int(last_r)
			else:
				last_b = b
				last_r = r
				s_max = max(s_max, s)
				b_max = b if s_max == s else b_max

	return int(b_max), int(num_hash_func / b_max)


# Get min. hashing for each business
def min_hashing(data, row_len, prime_vals):
	# Each business has a tuple of min. hashing values
	# the min. hashing on user index values for each business and for each hashing function
	sig_list = [min((p * id_user + 1) % row_len for id_user in data[1]) for p in prime_vals]

	return data[0], sig_list


# Mapping each business to a user list
def get_characteristic_map(data, users, businesses):
	cmap = dict()
	for line in data:
		id_b = businesses.index(line[1])
		id_u = users.index(line[0])
		cmap[id_b] = cmap.get(id_b, list()) + list([id_u])

	# zip_list = zip([businesses.index(b) for b in business_list],
	#                [users.index(u) for u in user_list])
	# cmap = dict(zip_list)

	return cmap


# Divide signature list of a business into bands and rows
def divide_band(data, band_len, row_len):
	b_id = data[0]
	min_hashing_list = data[1]
	band_list = list()

	# Divide the list of min. hashing values (user index) for one business
	for b in range(band_len):
		# Make row hashable
		row_tuple = tuple()
		for r in range(row_len):
			# Get each row of value in one band from the min. hashing list
			row_tuple += (min_hashing_list[b * row_len + r],)
		band_list.append(((b, row_tuple), b_id))

	return band_list


# Generate candidate pairs of the businesses that have at least one band in common
def generate_candidates(data):
	b_ids = data[1]
	return combinations(b_ids, 2)


# Get the Jaccard similarity of two businesses from the original data set
def get_jaccard_sim(data, bus_user_map, users, businesses):
	b_id_1 = data[0]
	b_id_2 = data[1]

	# Get user lists and turn into sets
	user_set_1 = set([users[idu] for idu in bus_user_map.get(b_id_1)])
	user_set_2 = set([users[idu] for idu in bus_user_map.get(b_id_2)])

	# Jaccard similarity = num(intersection) / num(union)
	jaccard_similarity = float(len(user_set_1.intersection(user_set_2)) /
	                           len(user_set_1.union(user_set_2)))

	bus_id_1 = businesses[b_id_1]
	bus_id_2 = businesses[b_id_2]
	if bus_id_1 < bus_id_2:
		return bus_id_1, bus_id_2, jaccard_similarity
	else:
		return bus_id_2, bus_id_1, jaccard_similarity


# Record data
def record_data(data, output_file):
	output_file.write("business_id_1, business_id_2, similarity\n")
	for item in data:
		output_file.write(str(item[0]) + "," + str(item[1]) + "," + str(item[2]) + "\n")
	# csv.register_dialect('md', delimiter=',',
	#                      quoting=csv.QUOTE_ALL, skipinitialspace=True,
	#                      lineterminator='', )
	# w = csv.writer(output_file, dialect='md')
	# field_names = ["business_id_1", "business_id_2", "similarity"]
	# w.writerow(field_names)
	# w.writerows(data)


# Main Execution
# Run Configurations
input_path = sys.argv[1]
output_path = sys.argv[2]
# Level of Parallelism - Recommended by Spark
# http://spark.apache.org/docs/latest/tuning.html#level-of-parallelism
cpu_num = multiprocessing.cpu_count()
task_per_cpu = cpu_num * 3

# Spark Configurations
# conf = SparkConf().setAppName('HW3 - Task 1').setMaster('local[*]')
conf = SparkConf() \
	.setAppName('HW3 - Task 1') \
	.setMaster('local[*]') \
	.set('spark.executor.memory', '8g') \
	.set('spark.driver.memory', '8g')
sc = SparkContext(conf=conf)

# Ground Truth
# ground_path = "C:/Users/Freedom/Documents/1_USC/Programs/inf553/HW3/Data/pure_jaccard_similarity.csv"
# ground = sc.textFile(ground_path) \
# 	.map(lambda x: x.split(",")) \
# 	.map(lambda x: (x[0], x[1]))
# g_list = ground.collect()

# precision = 0.0
# recall = 0.0
# it = 0
# performance_test_path = "./performance_test_path.txt"

start_time = time.time()

# Data Input
distFile = sc.textFile(input_path, minPartitions=2) \
	.coalesce(task_per_cpu) \
	.map(lambda s: s.split(","))

# Beheading
headers = distFile.first()
rdd = distFile.filter(lambda s: s != headers).persist()
# raw_data = rdd.map(lambda s: (s[0], s[1])).collect()
# print("raw data: " + str(time.time() - start_time))

# Getting all users and businesses
user = rdd.map(lambda s: s[0]).distinct().collect()
business = rdd.map(lambda s: s[1]).distinct().collect()
user_cnt = len(user)

# Constructing dictionaries for users and business index
user_enum = dict()
for i, u in enumerate(user):
	user_enum[u] = i

bus_enum = dict()
for i, b in enumerate(business):
	bus_enum[b] = i

# Group Users by Businesses Rated - Characteristic Map
characteristic_rdd = rdd \
	.map(lambda s: (bus_enum[s[1]], user_enum[s[0]])) \
	.groupByKey() \
	.mapValues(set)
# print("character matrix: " + str(time.time() - start_time))

# characteristic_map = get_characteristic_map(raw_data, user, business)
characteristic_map = dict(characteristic_rdd.collect())
# print("characteristic_map: " + str(time.time() - start_time))

hash_num = 28
# primes = list()
# band = 0
# row = 0

# with open(performance_test_path, 'w') as ptf:
# 	while precision < 1 or recall < 0.995 or it >= 1000:
# 		start_time = time.time()

# Signature Matrix
# primes = gen_primes(hash_num)
# 28 prime numbers - hash functions
# band, row = choose_band_row(hash_num)
band, row = 14, 2
primes = [554, 954, 638, 158, 191, 622, 802, 862, 320, 750, 814, 439, 790, 778,
          281, 523, 222, 761, 496, 496, 154, 990, 142, 136, 170, 986, 176, 738]

signature_matrix = characteristic_rdd \
	.map(lambda part: min_hashing(part, user_cnt, primes))

# print("Sig matrix: " + str(time.time() - start_time))

# LSH
# Final threshold requires 50%+ Jaccard similarity - choosing r = 3, b = 16 - s < 50%
# in order to reduce false negative
# print("Num of hash functions: " + str(hash_num))
# print("Band: " + str(band) + " Row: " + str(row))

# Divide signature matrix into bands of rows
divided_sig_mat = signature_matrix \
	.flatMap(lambda part: divide_band(part, band, row)) \
	.groupByKey() \
	.mapValues(list)

# print("divided sig matrix: " + str(time.time() - start_time))

# print("Divided sign mat")
# print(divided_sig_mat.take(5))

# Get candidates by filtering out the business pairs that have at least one band in common
candidates = divided_sig_mat \
	.filter(lambda s: len(s[1]) > 1) \
	.flatMap(lambda s: generate_candidates(s)) \
	.distinct()

# print("candidates: " + str(time.time() - start_time))

# print("candiates")
# print(candidates.take(5))

# Get Jaccard similarity of each business pair that is greater than 50%
jaccard_sim = candidates \
	.map(lambda s: get_jaccard_sim(s, characteristic_map, user, business)) \
	.filter(lambda s: s[2] >= 0.5) \
	.sortBy(lambda s: (s[0], s[1]))

# print("jaccard_sim: " + str(time.time() - start_time))

# Data Output
with open(output_path, 'w') as op:
	record_data(jaccard_sim.collect(), op)

print("Duration: " + str(time.time() - start_time))

# # Performance Measurement
# # Comparing ground truth with calculated
# js_candidates = jaccard_sim.map(lambda s: (s[0], s[1]))
# s_list = js_candidates.collect()
# # True Positive
# true_pos = ground.intersection(js_candidates)
# true_pos_list = true_pos.collect()
#
# # Precision and Recall
# precision = len(true_pos_list) / len(s_list)
# recall = len(true_pos_list) / len(g_list)
# print("Precision: " + str(precision))
# print("Recall: " + str(recall))
# print("========================")

		# it += 1
		#
		# ptf.write("Precision: " + str(precision) + " Recall: " + str(recall) + "\n")
		# ptf.write("Band: " + str(band) + " Row: " + str(row))
		# ptf.write(str(primes))
		# ptf.write("\n\n")
