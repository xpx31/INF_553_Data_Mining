import java.io.{BufferedWriter, File, FileWriter}
import System.nanoTime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object pengxiang_xu_task1 {
  def write_file(text: String, filename: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }

  def record_betweenness(data: Array[((String, String), Double)]): String ={
    var output_text = new StringBuilder()

    for (line <- data){
      output_text ++= "(\'" + line._1._1.toString + "\', \'" + line._1._2 + "\'), " +
        line._2.toString + "\n"
    }

    output_text.toString()
  }

  def record_community(data: Array[List[String]]): String ={
    var output_text = new StringBuilder()
    for (community <- data){
      for (idui <- community.indices) {
        if (idui < community.length - 1) {
          output_text ++= "\'" + community(idui) + "\', "
        } else {
          output_text ++= "\'" + community(idui) + "\'\n"
        }
      }
    }
    output_text.toString()
  }

  def convert_to_mutable(adj_mat_full: scala.collection.Map[Int, Set[Int]]):
    scala.collection.mutable.Map[Int, scala.collection.mutable.Set[Int]] ={
    val adj_mat: scala.collection.mutable.Map[Int, scala.collection.mutable.Set[Int]] =
      scala.collection.mutable.Map()

    for (pairs <- adj_mat_full) {
      adj_mat(pairs._1) = collection.mutable.Set(pairs._2.toSeq: _*)
    }

    adj_mat
  }

  def generate_user_pairs(data: (Int, List[Int]),
                          user: Array[String]): Iterator[(List[Int], Int)]= {
    val users = data._2.sortWith(user(_) < user(_))
    for (up <- users.combinations(2)) yield (up, 1)
  }

  def get_betweenness(root: Int,
                      adj_mat: scala.collection.mutable.Map[Int, scala.collection.mutable.Set[Int]],
                      user: Array[String]): List[((Int, Int), Double)] = {
    val bwtness: mutable.HashMap[(Int, Int), Double] = mutable.HashMap()
    val credit: mutable.HashMap[Int, Double] = mutable.HashMap()

    // BFS to find all weights of each node
    val bfs_info = bfs(root, adj_mat)
    val last_level = bfs_info._1
    val level_num = bfs_info._2
    val weight = bfs_info._3
    val parents = bfs_info._4

    // Assign betweenness
    for (lvl <- last_level until 0 by -1){
      for (node <- level_num(lvl)){
        credit(node) = credit.getOrElseUpdate(node, 1.0)
        for (parent <- parents(node)){
          credit(parent) = credit.getOrElseUpdate(parent, 1.0)

          var edge: (Int, Int) = (0, 0)
          if (user(parent) < user(node)){
            edge = (parent, node)
          } else {
            edge = (node, parent)
          }

          bwtness(edge) = credit(node) * weight(parent).toDouble / weight(node).toDouble
          credit(parent) += bwtness(edge)
        }
      }
    }

    // Turn betweenness into List
    var bwtness_list: List[((Int, Int), Double)] = List()
    for (s <- bwtness){
      bwtness_list ++= List(s)
    }

    bwtness_list
  }

  def bfs(root: Int,
          adj_mat: scala.collection.mutable.Map[Int, scala.collection.mutable.Set[Int]]):
              (Int,
                mutable.HashMap[Int, Set[Int]],
                mutable.HashMap[Int, Int],
                mutable.HashMap[Int, Set[Int]]) ={
    // Initialization
    val queue: mutable.Queue[Int] = mutable.Queue()
    val weight: mutable.HashMap[Int, Int] = mutable.HashMap()
    val parents: mutable.HashMap[Int, Set[Int]] = mutable.HashMap()
    val level_node: mutable.HashMap[Int, Int] = mutable.HashMap()
    val level_num: mutable.HashMap[Int, Set[Int]] = mutable.HashMap()
    var visited: Set[Int] = Set()
    var last_level = 0

    // Add Root Node
    queue.enqueue(root)
    visited += root
    level_node(root) = 0
    level_num(last_level) = Set(root)
    weight(root) = 1

    // BFS
    while (queue.nonEmpty){
      val node = queue.dequeue()
      val children = adj_mat(node)

      // Loop through all children of the node at current level node
      for (child <- children){
        if (!visited.contains(child)){
          queue.enqueue(child)
          visited += child
          weight(child) = weight(node)
          parents(child) = parents.getOrElseUpdate(child, Set()) ++ Set(node)

          // Level Information
          level_node(child) = level_node(node) + 1
          level_num(level_node(child)) = level_num.getOrElse(level_node(child), Set()) ++ Set(child)
          last_level = math.max(last_level, level_node(child))
        } else {
          // Edges between nodes on the same level_node cannot be included
          if (child != root && level_node(child) == level_node(node) + 1){
            // Add parent
            parents(child) += node
            // Sum all weights from parents
            weight(child) += weight(node)
          }
        }
      }
    }

    (last_level, level_num, weight, parents)
  }

  def remove_edge(adj_mat: scala.collection.mutable.Map[Int, scala.collection.mutable.Set[Int]],
                  edge_remove: (Int, Int)): Unit={
    val vertex_1 = edge_remove._1
    val vertex_2 = edge_remove._2
    val adj_mat_v1_new = adj_mat(vertex_1) - vertex_2
    val adj_mat_v2_new = adj_mat(vertex_2) - vertex_1
    adj_mat(vertex_1) = adj_mat_v1_new
    adj_mat(vertex_2) = adj_mat_v2_new
  }

  def get_modularity(adj_mat: scala.collection.mutable.Map[Int, scala.collection.mutable.Set[Int]],
                     k: scala.collection.Map[Int, Int],
                     m: Int): (Double, List[List[Int]])={
    var modu: Double = 0.0

    // Get connected vertices
    val vertices_connected_list = get_connected_vertices(adj_mat)

    // Determine modularity
    for (vertices_list <- vertices_connected_list){
      for (i <- vertices_list){
        for (j <- vertices_list){
          var a_ij = 0
          if (adj_mat(i).contains(j)){
            a_ij = 1
          }
          modu += a_ij - (k(i) * k(j)).toDouble / (2 * m).toDouble
        }
      }
    }
    modu /= (2 * m).toDouble
    (modu, vertices_connected_list)
  }

  def get_connected_vertices(adj_mat: scala.collection.mutable.Map[Int, scala.collection.mutable.Set[Int]]): List[List[Int]] ={
    var visited: Set[Int] = Set()
    var connected_list: List[List[Int]] = List()

    // Loop through all vertices in the adjacency matrix (dict)
    for (node_dict <- adj_mat){
      val vertex = node_dict._1
      if (!visited.contains(vertex)){
        val q: mutable.Queue[Int] = mutable.Queue()
        q.enqueue(vertex)
        visited += vertex
        var con_list: List[Int] = List(vertex)

        // BFS
        while (q.nonEmpty) {
          val node = q.dequeue()
          val connected = adj_mat(node)

          for (c <- connected){
            if (!visited.contains(c)){
              q.enqueue(c)
              visited += c
              con_list ++= List(c)
            }
          }
        }
        connected_list ++= List(con_list)
      }
    }

    connected_list
  }

  def convert_user(data: List[Int], user: Array[String]): List[String] ={
    var user_str: List[String] = List()
    for (u <- data){
      user_str ++= List(user(u))
    }

    user_str.sortWith(_ < _)
  }

  def main(args: Array[String]): Unit = {
    val start = nanoTime()

    Logger.getLogger("org").setLevel(Level.FATAL)

    // Run Configuration
    val filter_th = args(0).toInt
    val input_path = args(1)
    val betweenness_path = args(2)
    val community_path = args(3)
    val ava_cpu_num = Runtime.getRuntime.availableProcessors()
    val task_per_cpu = ava_cpu_num * 3

    // Spark Configuration
    val conf = new SparkConf()
      .setAppName("HW3 - Task 1")
      .setMaster("local[*]")
      .set("spark.executor.memory", "8g")
      .set("spark.driver.memory", "8g")
    val sc = new SparkContext(conf)

    // Input Data
    val distFile = sc.textFile(input_path).coalesce(task_per_cpu)

    // Beheading
    val headers = distFile.first
    val rdd = distFile.filter(_ != headers)
      .map(s => s.split(","))
      .persist()

    // Getting all users and businesses
    val user = rdd.map(s => s(0)).distinct().collect()
    val business = rdd.map(s => s(1)).distinct().collect()

    // Constructing dictionaries for users and business index
    val user_map: mutable.HashMap[String, Int] = mutable.HashMap()
    var i = 0
    for (u <- user){
      user_map.update(u, i)
      i += 1
    }

    val bus_map: mutable.HashMap[String, Int] = mutable.HashMap()
    i = 0
    for (b <- business){
      bus_map.update(b, i)
      i += 1
    }

    // Create all user pairs that has connections greater than the filter threshold
    val user_pairs = rdd
      .map(s => (bus_map(s(1)), List(user_map(s(0)))))
      .reduceByKey(_ ++ _)
      .flatMap(s => generate_user_pairs(s, user))
      .reduceByKey(_ + _)
      .filter(s => s._2 >= filter_th)
      .map(s => s._1)

    // Create all vertices from user pairs
    val vertices = user_pairs
      .flatMap(s => s.toSet)
      .distinct()

    // Create the adjacency matrix
    val adj_upper_right = user_pairs
      .map(s => (s.head, Set(s(1))))
      .reduceByKey(_ ++ _)
    val adj_lower_left = user_pairs
      .map(s => (s(1), Set(s.head)))
      .reduceByKey(_ ++ _)
    val adj_matrix_full = adj_upper_right
      .union(adj_lower_left)
      .reduceByKey(_ ++ _)
      .collectAsMap()

    // Convert Adjacency matrix into mutable map for further processing
    val adj_matrix: scala.collection.mutable.Map[Int, scala.collection.mutable.Set[Int]] =
      convert_to_mutable(adj_matrix_full)

    // Girven-Newman Algorithm
    val betweenness = vertices
      .flatMap(s => get_betweenness(s, adj_matrix, user))
      .reduceByKey(_ + _)
      .map(s => ((user(s._1._1), user(s._1._2)), s._2 / 2.0))
      .sortBy(s => (-s._2, s._1))
      .collect()

    // Split into Communities
    // Get original number of versions connected to each vertex
    val adj_len_dict: mutable.HashMap[Int, Int] = mutable.HashMap()
    for (s <- adj_matrix){
      adj_len_dict(s._1) = s._2.size
    }

    // Get the total length of edges
    val edge_len = betweenness.length

    // Calculate modularity and find max.
    val node_1 = user_map(betweenness.head._1._1)
    val node_2 = user_map(betweenness.head._1._2)
    var edge_remove = (node_1, node_2)
    var mod_max: Double = 0.0
    var community_list: List[List[Int]] = List()
    for (cnt <- 0 to edge_len) {
      remove_edge(adj_matrix, edge_remove)
      val mod_rst = get_modularity(adj_matrix, adj_len_dict, edge_len)
      val mod = mod_rst._1
      val com_list = mod_rst._2

      // Get Max Modularity
      if (mod > mod_max){
        mod_max = mod
        community_list = com_list.map(s => s)
      }

      // Calculate betweenness again
      if (cnt < edge_len - 1){
        val betweenness_top = vertices
          .flatMap(s => get_betweenness(s, adj_matrix, user))
          .reduceByKey(_ + _)
          .map(s => (s._1, s._2 / 2.0))
          .sortBy(s => -s._2)
          .take(1)
        edge_remove = betweenness_top.map(s => s._1).head
      }
    }

    // Sort user ids
    val community_rdd = sc.parallelize(community_list)
    val community = community_rdd
      .map(s => convert_user(s, user))
      .sortBy(s => (s.length, s.head))
      .collect()

    // Data Output
    val betweenness_output_text = record_betweenness(betweenness)
    write_file(betweenness_output_text, betweenness_path)

    val community_output_text = record_community(community)
    write_file(community_output_text, community_path)
    val duration = (nanoTime() - start) / 1e9
    println("Duration: " + duration)
  }
}