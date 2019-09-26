import math
from pyspark import SparkContext, SparkConf
import sys
import time
import multiprocessing
from pyspark.mllib.recommendation import ALS, Rating
import heapq

start_time = time.time()


# Model based cf
def model_base_cf(rdd, train_rdd, test_rdd, user_enum, bus_enum):
	# Training
	ratings_train = train_rdd \
		.map(lambda s: Rating(user_enum[s[0]], bus_enum[s[1]], float(s[2])))
	rank = 3
	num_iter = 20
	model = ALS.train(ratings_train, rank, num_iter, 0.265)

	# Evaluation
	test_features = test_rdd \
		.map(lambda s: (user_enum[s[0]], bus_enum[s[1]]))
	prediction = model \
		.predictAll(test_features) \
		.map(lambda s: ((s[0], s[1]), s[2]))

	ratings_test = test_rdd \
		.map(lambda s: Rating(user_enum[s[0]], bus_enum[s[1]], float(s[2])))
	rate_pred = ratings_test \
		.map(lambda s: ((s[0], s[1]), s[2])) \
		.join(prediction) \
		.cache()

	# Filling users in test and not in training into prediction by default rating, 3
	d_rating = float(3.0)
	rdd_u = rdd.map(lambda s: (user_enum[s[0]], (bus_enum[s[1]], float(s[2]))))
	rp_u = train_rdd.map(lambda s: (user_enum[s[0]], (bus_enum[s[1]], float(s[2]))))
	rate_pred_u = rdd_u.subtractByKey(rp_u) \
		.map(lambda s: ((s[0], s[1][0]), (s[1][1], d_rating)))

	# Filling businesses in test and not in training into prediction by default rating, 3
	rdd_b = rdd.map(lambda s: (bus_enum[s[1]], (user_enum[s[0]], float(s[2]))))
	rp_b = train_rdd.map(lambda s: (bus_enum[s[1]], (user_enum[s[0]], float(s[2]))))
	rate_pred_b = rdd_b.subtractByKey(rp_b) \
		.map(lambda s: ((s[1][0], s[0]), (s[1][1], d_rating)))

	# Combine all
	rate_pred_diff = rate_pred_u.union(rate_pred_b)
	rate_pred = rate_pred.union(rate_pred_diff)

	return rate_pred


#############################################################
# User based cf
def user_base_cf(case_num, train_rdd, test_rdd, user_enum, bus_enum):
	# Business to user map
	bus_user_map = train_rdd \
		.map(lambda s: (bus_enum[s[1]], [user_enum[s[0]]])) \
		.reduceByKey(lambda x, y: x + y) \
		.collectAsMap()

	# User information
	user_bus = train_rdd \
		.map(lambda s: (user_enum[s[0]], [bus_enum[s[1]]])) \
		.reduceByKey(lambda x, y: x + y)
	user_rating = train_rdd \
		.map(lambda s: (user_enum[s[0]], [float(s[2])])) \
		.reduceByKey(lambda x, y: x + y) \
		.map(lambda s: normalize_rating(s))
	user_bus_rating = user_bus.join(user_rating) \
		.map(lambda s: ((s[0], s[1][0]), (s[1][1][0], s[1][1][1])))

	# User to business map
	user_bus_map = user_bus.collectAsMap()

	# User and business to ratings map
	user_bus_rating_map = user_bus_rating \
		.flatMap(lambda s: expand_ratings(case_num, s)) \
		.collectAsMap()

	# Test Features
	test_features = test_rdd.map(lambda s: (user_enum[s[0]], bus_enum[s[1]]))

	# Take top n number of cos similarities
	# 13, 1.091
	top = 13

	# Prediction
	prediction = test_features \
		.map(lambda s: predict_rating(case_num, s, user_bus_rating_map, user_bus_map, bus_user_map, top))

	rate_pred = test_rdd \
		.map(lambda s: ((user_enum[s[0]], bus_enum[s[1]]), float(s[2]))) \
		.join(prediction)

	return rate_pred


#############################################################
# Item based cf
def item_base_cf(case_num, train_rdd, test_rdd, user_enum, bus_enum):
	# User to business map
	user_bus_map = train_rdd \
		.map(lambda s: (user_enum[s[0]], [bus_enum[s[1]]])) \
		.reduceByKey(lambda x, y: x + y) \
		.collectAsMap()

	# Business information
	bus_user = train_rdd \
		.map(lambda s: (bus_enum[s[1]], [user_enum[s[0]]])) \
		.reduceByKey(lambda x, y: x + y)
	bus_rating = train_rdd \
		.map(lambda s: (bus_enum[s[1]], [float(s[2])])) \
		.reduceByKey(lambda x, y: x + y) \
		.map(lambda s: normalize_rating(s))
	user_bus_rating = bus_user.join(bus_rating) \
		.map(lambda s: ((s[1][0], s[0]), (s[1][1][0], s[1][1][1])))

	# User and business to ratings map
	user_bus_rating_map = user_bus_rating \
		.flatMap(lambda s: expand_ratings(case_num, s)) \
		.collectAsMap()

	# User Information
	bus_user_map = bus_user.collectAsMap()

	# Test Features
	test_features = test_rdd.map(lambda s: (user_enum[s[0]], bus_enum[s[1]]))

	# rmse = 10
	# rate_pred = None
	top = 6

	# while rmse > 0.9:
	# start_time = time.time()
	# Prediction
	prediction = test_features \
		.map(lambda s: predict_rating(case_num, s, user_bus_rating_map, user_bus_map, bus_user_map, top))

	rate_pred = test_rdd \
		.map(lambda s: ((user_enum[s[0]], bus_enum[s[1]]), float(s[2]))) \
		.join(prediction)

	# print("prediction: ", str(time.time() - start_time))
	# # RMSE
	# rmse = evaluate_rmse(rate_pred)
	# print("Top: ", top)
	# top *= 2

	return rate_pred


# Record data
def record_data(data, output_file, user, business):
	output_file.write("user_id, business_id, prediction\n")

	# Model-based cf
	data_list = data.collect()
	for line in data_list:
		output_file.write(user[line[0][0]] + "," + business[line[0][1]] + "," + str(line[1][1]) + "\n")


# Create user and item dictionaries associate with their index in input list
def create_user_item_dict(user_list, item_list):
	user_enum = dict()
	for i, u in enumerate(user_list):
		user_enum[u] = i

	bus_enum = dict()
	for i, b in enumerate(item_list):
		bus_enum[b] = i

	return user_enum, bus_enum


# Print the RSME of predicted ratings
def evaluate_rmse(rate_pred):
	rmse = math.sqrt(rate_pred
	                 .map(lambda s: ((s[1][0] - s[1][1]) ** 2))
	                 .mean())
	print("RMSE: " + str(rmse))
	return rmse


# Normalize rating for each user
def normalize_rating(data):
	# Field could be user or business
	field = data[0]
	ratings_old = data[1]
	ratings_avg = 0.0

	# Get average rating
	for r in ratings_old:
		ratings_avg += float(r)
	ratings_avg /= len(ratings_old)

	return field, (ratings_old, ratings_avg)


# Expand the map for each business with one user
def expand_ratings(case_num, data):
	user = data[0][0]
	busis = data[0][1]
	ratings = data[1][0]
	avg_rating = data[1][1]

	expand_list = list()
	if case_num == 2:
		# User based cf
		for i, b in enumerate(busis):
			expand_list.append(((user, b), (ratings[i], avg_rating)))
	elif case_num == 3:
		# Item based cf
		for i, u in enumerate(user):
			expand_list.append(((u, busis), (ratings[i], avg_rating)))

	return expand_list


def predict_rating(case_num, test_fea, ubr_map, ub_map, bu_map, top_n):
	user = int(test_fea[0])
	busi = int(test_fea[1])

	# Business in testing but not in training
	if busi not in bu_map and user not in ub_map:
		return (user, busi), 3.0
	elif busi not in bu_map:
		# Average of the other businesses that the user rated
		return (user, busi), float(ubr_map[(user, ub_map[user][0])][1])
	elif user not in ub_map:
		# Average of the business rating from other users
		return (user, busi), float(ubr_map[(bu_map[busi][0], busi)][1])
	else:
		# Business in training as well
		field_list, co_rate = list(), list()
		field_avg = 0.0, 0.0
		if case_num == 2:
			# User based cf
			field_list = ub_map.get(user)
			field_avg = float(ubr_map[(user, field_list[0])][1])
			co_rate = bu_map[busi]
		elif case_num == 3:
			# Item based cf
			field_list = bu_map.get(busi)
			field_avg = float(ubr_map[(field_list[0], busi)][1])
			co_rate = ub_map[user]

		cos_sims = get_cos_sims(case_num, co_rate, field_list, field_avg, ubr_map, ub_map, bu_map, user, busi, top_n)
		rate_pred = get_rating_pred_cs(case_num, cos_sims, ubr_map, field_avg, user, busi)
		return rate_pred


# Get cos similarities
def get_cos_sims(case_num, co_rate, field_list, rate_avg, ubr_map, ub_map, bu_map, user, busi, top_n):
	cos_sims = list()
	rate_avg_2 = 0.0
	bus_co_rate_len = 0

	# Find cos similarities of the co-rated users or business
	for f in co_rate:
		other_field_co_rate = set(field_list)
		if case_num == 2:
			other_field_co_rate &= set(ub_map[f])
			rate_avg_2 = ubr_map[(f, ub_map[f][0])][1]
		elif case_num == 3:
			# # Skip the users who did not rate on business
			# if ubr_map.get((user, busi)) is None:
			# 	continue

			other_field_co_rate &= set(bu_map[f])
			bus_co_rate_len = len(other_field_co_rate)
			rate_avg_2 = ubr_map[(bu_map[f][0], f)][1]

		num_cs, den_cs_1, den_cs_2 = 0.0, 0.0, 0.0

		# Calculate the numerator and denominator of each cos similarity
		for of in other_field_co_rate:
			r_1, r_2 = 0.0, 0.0
			if case_num == 2:
				if of != busi:
					r_1 = float(ubr_map[(user, of)][0]) - rate_avg
					r_2 = float(ubr_map[(f, of)][0]) - rate_avg_2
			elif case_num == 3:
				if of != user:
					r_1 = float(ubr_map[(of, busi)][0]) - rate_avg
					r_2 = float(ubr_map[(of, f)][0]) - rate_avg_2

			num_cs += r_1 * r_2
			den_cs_1 += r_1 ** 2
			den_cs_2 += r_2 ** 2

		# Calcualte cos. similarity
		cos_sim = num_cs / math.sqrt(den_cs_1 * den_cs_2) if num_cs != 0 else 0

		# Memory-Based improvement
		if case_num == 3:
			# if cos_sim <= 0.0:
			# 	continue

			# Case Amplification
			# cos_sim *= abs(cos_sim) ** 1.5
			# Default Voting
			cos_sim *= 0 if bus_co_rate_len < 60 else 1

		cos_sims.append((f, cos_sim, rate_avg_2))

	# Take top n cos similarities
	cos_sims = heapq.nlargest(top_n, cos_sims, key=lambda s: s[1])
	return cos_sims


# Get the rating prediction from cos. similarities
def get_rating_pred_cs(case_num, cos_sims, ubr_map, rate_avg, user, busi):
	num_w, den_w, r = 0, 0, 0.0

	# Get weights
	for cs in cos_sims:
		field_cs = cs[0]
		cos_sim = cs[1]
		rating_cs = cs[2]

		if case_num == 2:
			# User based cf
			# rating_cs is average rating of business coloumn for every other users
			r = float(ubr_map[(field_cs, busi)][0]) - rating_cs \
				if ubr_map.get((field_cs, busi)) is not None else 0.0
		elif case_num == 3:
			# Item based cf
			# r is the rating of business for every other user
			r = ubr_map.get((user, field_cs))[0]

		num_w += r * cos_sim
		den_w += abs(cos_sim)

	# Weighted rating
	rating_pred = rate_avg
	if den_w != 0:
		if case_num == 2:
			# User based cf
			rating_pred = rate_avg + num_w / den_w
		elif case_num == 3:
			# Item based cf
			rating_pred = num_w / den_w

	# Round ratings
	# if rating_pred < 1.0:
	# 	rating_pred = 1.0
	# elif rating_pred > 5.0:
	# 	rating_pred = 5.0
	# else:
	# 	rating_pred = round(rating_pred, 1)

	return (user, busi), rating_pred


# Main Execution
# Run Configurations
train_path = sys.argv[1]
test_path = sys.argv[2]
case_id = int(sys.argv[3])
output_path = sys.argv[4]
# Level of Parallelism - Recommended by Spark
# http://spark.apache.org/docs/latest/tuning.html#level-of-parallelism
cpu_num = multiprocessing.cpu_count()
task_per_cpu = cpu_num * 3

# Spark Configurations
# conf = SparkConf().setAppName('HW3 - Task 1').setMaster('local[*]')
conf = SparkConf() \
	.setAppName('HW3 - Task 2') \
	.setMaster('local[*]') \
	.set('spark.executor.memory', '8g') \
	.set('spark.driver.memory', '8g')
sc = SparkContext(conf=conf)

# Data Input
distFile_train = sc.textFile(train_path) \
	.coalesce(task_per_cpu) \
	.map(lambda s: s.split(","))
distFile_test = sc.textFile(test_path) \
	.coalesce(task_per_cpu) \
	.map(lambda s: s.split(","))

# Beheading
headers = distFile_train.first()
rdd_train = distFile_train.filter(lambda s: s != headers).cache()
rdd_test = distFile_test.filter(lambda s: s != headers).cache()

# All users and businesses from both training and testing rdds
rdd = sc.union([rdd_train, rdd_test]).cache()
users = rdd.map(lambda s: s[0]).distinct().collect()
busis = rdd.map(lambda s: s[1]).distinct().collect()

# Constructing dictionaries for users and business index
user_enum, bus_enum = create_user_item_dict(users, busis)

rst = None

# Model Based CF
if case_id == 1:
	rst = model_base_cf(rdd, rdd_train, rdd_test, user_enum, bus_enum)
elif case_id == 2:
	# User Based CF
	rst = user_base_cf(case_id, rdd_train, rdd_test, user_enum, bus_enum)
elif case_id == 3:
	# Item Based CF
	rst = item_base_cf(case_id, rdd_train, rdd_test, user_enum, bus_enum)

if rst is not None:
	# Data output
	with open(output_path, "w") as op:
		record_data(rst, op, users, busis)

print("Duration: " + str(time.time() - start_time))

# if rst is not None:
# 	# RMSE
# 	evaluate_rmse(rst)
