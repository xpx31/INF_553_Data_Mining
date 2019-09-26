from queue import Queue
from pyspark import SparkContext, SparkConf
import sys
import time
import multiprocessing
from itertools import combinations

start_time = time.time()


# Record data
def record_betweenness(data, output_file):
	for item in data:
		output_file.write(str(item[0]) + ", " + str(item[1]) + "\n")


def record_community(data, output_file):
	for community in data:
		for idui, u_id_str in enumerate(community):
			if idui < len(community) - 1:
				output_file.write("\'" + u_id_str + "\', ")
			else:
				output_file.write("\'" + u_id_str + "\'\n")


def generate_user_pairs(it, user_list):
	users = sorted(it[1], key=lambda x: user_list[x])

	for up in combinations(users, 2):
		yield (up), 1


def get_betweenness(root, adj_mat, user_list):
	bwtness = dict()
	credit = dict()

	# BFS to find all weights of each node
	last_level, level_num, weight, parents = bfs(root, adj_mat)

	# Assign betweenness
	for lvl in range(last_level, 0, -1):
		for node in level_num[lvl]:
			credit[node] = credit.get(node, 1.0)
			for parent in parents[node]:
				credit[parent] = credit.get(parent, 1.0)
				edge = tuple((parent, node)) if user_list[parent] < user_list[node] \
					else tuple((node, parent))

				bwtness[edge] = credit[node] * float(weight[parent]) / float(weight[node])
				credit[parent] += bwtness[edge]

	# Turn betweenness into list
	bwtness_list = list()
	for k, v in bwtness.items():
		bwtness_list.append((k, v))

	return bwtness_list


def bfs(root, adjm):
	# Initialization
	queue = Queue()
	weight, parents, level_node, level_num = dict(), dict(), dict(), dict()
	last_level = 0

	# Add root node
	queue.put(root)
	visited = {root}
	level_node[root] = 0
	level_num[last_level] = {root}
	weight[root] = 1

	# BFS
	while not queue.empty():
		node = queue.get()
		children = adjm[node]

		# Loop through all children of the node at current level_node
		for child in children:
			if child not in visited:
				queue.put(child)
				visited.add(child)
				weight[child] = weight[node]
				parents[child] = parents.get(child, set()) | {node}

				# Level information
				level_node[child] = level_node[node] + 1
				level_num[level_node[child]] = level_num.get(level_node[child], set()) | {child}
				last_level = max(last_level, level_node[child])
			else:
				# Edges between nodes on the same level_node cannot be included
				if child != root and level_node[child] == level_node[node] + 1:
					# Add parent
					parents[child] |= {node}
					# Sum all weights from parents
					weight[child] += weight[node]

	return last_level, level_num, weight, parents


def remove_edge(adj_mat, edge_to_remove):
	vertex_1 = edge_to_remove[0]
	vertex_2 = edge_to_remove[1]
	adj_mat[vertex_1].remove(vertex_2)
	adj_mat[vertex_2].remove(vertex_1)


def get_modularity(adj_mat, k, m):
	modu = 0

	# Get connected vertices
	vertices_connected_list = get_connected_vertices(adj_mat)

	# Determine modularity
	for vertices_list in vertices_connected_list:
		for i in vertices_list:
			for j in vertices_list:
				a_ij = 1 if j in adj_mat[i] else 0
				modu += a_ij - (k[i] * k[j]) / (2 * m)

	modu /= (2 * m)
	return modu, vertices_connected_list


def get_connected_vertices(matrix):
	visited = set()
	connected_list = list()

	# Loop through all vertices in the adjacency matrix (dict)
	for vertex in matrix:
		if vertex not in visited:
			q = Queue()
			q.put(vertex)
			visited.add(vertex)
			con_list = [vertex]

			# BFS
			while not q.empty():
				node = q.get()
				connected = matrix[node]

				for c in connected:
					if c not in visited:
						q.put(c)
						visited.add(c)
						con_list.append(c)

			connected_list.append(con_list)

	return connected_list


# Main Execution
# Run Configurations
filter_th = int(sys.argv[1])
input_path = sys.argv[2]
betweenness_path = sys.argv[3]
community_path = sys.argv[4]
# Level of Parallelism - Recommended by Spark
# http://spark.apache.org/docs/latest/tuning.html#level_node-of-parallelism
cpu_num = multiprocessing.cpu_count()
task_per_cpu = cpu_num * 3

# Spark Configurations
# conf = SparkConf().setAppName('HW3 - Task 1').setMaster('local[*]')
conf = SparkConf() \
	.setAppName('HW4 - Task 1') \
	.setMaster('local[*]') \
	.set('spark.executor.memory', '8g') \
	.set('spark.driver.memory', '8g')
sc = SparkContext(conf=conf)


# Data Input
distFile = sc.textFile(input_path) \
	.coalesce(task_per_cpu) \
	.map(lambda s: s.split(","))

# Beheading
headers = distFile.first()
rdd = distFile.filter(lambda s: s != headers).persist()

# Getting all users and businesses
user = rdd.map(lambda s: s[0]).distinct().collect()
business = rdd.map(lambda s: s[1]).distinct().collect()

# Constructing dictionaries for users and business index
user_map = dict()
for idu, u in enumerate(user):
	user_map[u] = idu

bus_map = dict()
for idb, b in enumerate(business):
	bus_map[b] = idb

# Create all user pairs that has connections greater than the filter threshold
user_pairs = rdd \
	.map(lambda s: (bus_map[s[1]], [user_map[s[0]]]))\
	.reduceByKey(lambda x, y: x + y)\
	.flatMap(lambda s: generate_user_pairs(s, user))\
	.reduceByKey(lambda x, y: x + y)\
	.filter(lambda s: s[1] >= filter_th) \
	.map(lambda s: s[0])

# print("user_pairs: " + str(time.time() - start_time))

# Create all vertices from user pairs
vertices = user_pairs\
	.flatMap(lambda s: {s[0], s[1]}) \
	.distinct()

# Create the adjacency matrix
adj_upper_right = user_pairs \
	.map(lambda s: (s[0], {s[1]})) \
	.reduceByKey(lambda x, y: x | y)

adj_lower_left = user_pairs \
	.map(lambda s: (s[1], {s[0]})) \
	.reduceByKey(lambda x, y: x | y)

adj_matrix = adj_upper_right\
	.union(adj_lower_left)\
	.reduceByKey(lambda x, y: x | y) \
	.collectAsMap()

# print("adj_matrix: " + str(time.time() - start_time))

# Girvan-Newman Algorithm
betweenness = vertices\
	.flatMap(lambda s: get_betweenness(s, adj_matrix, user)) \
	.reduceByKey(lambda x, y: x + y) \
	.map(lambda s: ((user[s[0][0]], user[s[0][1]]), s[1] / 2.0)) \
	.sortBy(lambda s: (-s[1], s[0])) \
	.collect()

# Data Output
with open(betweenness_path, 'w') as op:
	record_betweenness(betweenness, op)

# Split into Communities
# Get original number of vertices connected to each vertex
adj_len_dict = dict()
for key, v_list in adj_matrix.items():
	adj_len_dict[key] = len(v_list)

# Get the total length of edges
edge_len = len(betweenness)

# Calculate modularity and find max.
edge_remove = (user_map[betweenness[0][0][0]], user_map[betweenness[0][0][1]])
mod_max = 0
community_list = betweenness
# mod_max, community_list = get_modularity(adj_matrix, adj_len_dict, edge_len)
for cnt in range(edge_len):
	remove_edge(adj_matrix, edge_remove)
	mod, com_list = get_modularity(adj_matrix, adj_len_dict, edge_len)

	# Get max modularity
	if mod > mod_max:
		mod_max = mod
		community_list = com_list.copy()

	# Calculate betweenness again
	if cnt < edge_len - 1:
		betweenness_top = vertices \
			.flatMap(lambda s: get_betweenness(s, adj_matrix, user)) \
			.reduceByKey(lambda x, y: x + y) \
			.map(lambda s: ((s[0]), s[1] / 2.0)) \
			.sortBy(lambda s: -s[1]) \
			.take(1)
		edge_remove = betweenness_top[0][0]

# Sort user ids
community_rdd = sc.parallelize(community_list)
community = community_rdd\
	.map(lambda s: sorted([user[x] for x in s])) \
	.sortBy(lambda s: (len(s), s[0])) \
	.collect()

# Data Output
with open(betweenness_path, 'w') as op:
	record_betweenness(betweenness, op)

with open(community_path, 'w') as op:
	record_community(community, op)

print("Duration: " + str(time.time() - start_time))
