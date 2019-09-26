import java.io.{BufferedWriter, File, FileWriter}
import System.nanoTime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object pengxiang_xu_task1 {
  def write_file(text: String, filename: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }

  // Get min. hashing for each business
  def min_hashing(data: (Int, List[Int]), row_len: Int, prime_vals: List[Int]): (Int, List[Int]) ={
    // Each business has a List of min. hashing values
    // the min. hashing on user index values for each business and for each hashing function
    val sig_list = new ListBuffer[Int]()
    for (p <- prime_vals) {
      var id_min = row_len
      for (id_user <- data._2) {
        id_min = math.min(id_min, (p * id_user + 1) % row_len)
      }
      sig_list += id_min
    }

    (data._1, sig_list.toList)
  }

  // Divide signature list of a business into bands and rows
  def divide_band(data: (Int, List[Int]), band: Int, row: Int): List[((Int, List[Int]), Int)] ={
    val b_id = data._1
    val min_hashing_list = data._2
    val band_list = new ListBuffer[((Int, List[Int]), Int)]()

    // Divide the list of min. hashing values (user index) for one business
    for (b <- 0 until band){
      val row_items = new ListBuffer[Int]()
      for (r <- 0 until row){
        // Get each row of values in one band from the min. hashing list
        row_items += min_hashing_list(b * row + r)
      }
      band_list += (((b, row_items.toList), b_id))
    }

    band_list.toList
  }

  // Generate candidate pairs of the businesses that have at least one band in common
  def generate_candidates(data: ((Int, List[Int]), List[Int])): Iterator[List[Int]] ={
    data._2.combinations(2)
  }

  def get_jaccard_sim(data: List[Int], bu_map: Map[Int, List[Int]],
                      users: Array[String], businesses: Array[String]): (String, String, Double) ={
    val b_id_1 = data(0)
    val b_id_2 = data(1)

    val bus_id_1 = businesses(b_id_1)
    val bus_id_2 = businesses(b_id_2)

    // Get user lists and turn into sets
    val user_list_1 = new ListBuffer[String]()
    val user_list_2 = new ListBuffer[String]()
    val b_list_1 = bu_map.get(b_id_1)
    val b_list_2 = bu_map.get(b_id_2)

    b_list_1.foreach(idu => idu.foreach(user_list_1 += users(_)))
    b_list_2.foreach(idu => idu.foreach(user_list_2 += users(_)))

    val user_set_1 = user_list_1.toList.toSet
    val user_set_2 = user_list_2.toList.toSet

    // Jaccard Similarity
    val intersection_len = user_set_1.intersect(user_set_2).size
    val union_len = user_set_1.union(user_set_2).size
    val jaccard_sim: Double = intersection_len.toDouble / union_len.toDouble

    if (bus_id_1 < bus_id_2){
      (bus_id_1, bus_id_2, jaccard_sim)
    } else {
      (bus_id_2, bus_id_1, jaccard_sim)
    }
  }

  def main(args: Array[String]): Unit = {
    val start = nanoTime()

    Logger.getLogger("org").setLevel(Level.FATAL)

    // Run Configuration
    val input_path = args(0)
    val output_path = args(1)
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
    val distFile = sc.textFile(input_path, minPartitions = 2)
      .coalesce(task_per_cpu)

    // Beheading
    val headers = distFile.first
    val rdd = distFile.filter(_ != headers)
      .map(s => s.split(","))
      .persist()

    // Getting all users and businesses
    val user = rdd.map(s => s(0)).distinct().collect()
    val business = rdd.map(s => s(1)).distinct().collect()
    val user_cnt = user.length

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

    // Group Users by Businesses Rated - Characteristic Map
    val characteristic_rdd = rdd
      .map(s => (bus_map(s(1)), List(user_map(s(0)))))
      .reduceByKey(_ ++ _)

    var characteristic_map = characteristic_rdd.collect().toMap

    // Signature Matrix
    val primes = List(554, 954, 638, 158, 191, 622, 802, 862, 320, 750, 814, 439, 790, 778,
      281, 523, 222, 761, 496, 496, 154, 990, 142, 136, 170, 986, 176, 738)
    val band = 14
    val row = 2
    val sigature_matrix = characteristic_rdd
      .map(s => min_hashing(s, user_cnt, primes))

    // LSH
    // Final threshold requires 50%+ Jaccard similarity - choosing r = 3, b = 16 - s < 50%
    // in order to reduce false negative
    val divided_sig_mat = sigature_matrix
      .flatMap(p => divide_band(p, band, row))
      .groupByKey().mapValues(s => s.toList)

    // Get candidates by filtering out the business pairs that have at least one band in common
    val candidates = divided_sig_mat
      .filter(s => s._2.length > 1)
      .flatMap(s => generate_candidates(s))
      .distinct()

    // Get Jaccard similarity of each business pair that is greater than 50%
    val jaccard_sim = candidates
      .map(s => get_jaccard_sim(s, characteristic_map, user, business))
      .filter(s => s._3 >= 0.5)
      .sortBy(s => (s._1, s._2))

    // Output Data
    val j_sims = jaccard_sim.collect()
    var output_text = "business_id_1, business_id_2, similarity\n"
    for (js <- j_sims){
      output_text ++= js._1 + "," + js._2 + "," + js._3 + "\n"
    }

    write_file(output_text, output_path)
    val duration = (nanoTime() - start) / 1e9
    println("Duration: " + duration)

    // Compare with Ground Truth
//    val ground_path = "C:/Users/Freedom/Documents/1_USC/Programs/inf553/HW3/Data/pure_jaccard_similarity.csv"
//    val ground = sc.textFile(ground_path)
//      .map(s => s.split(","))
//      .map(s => (s(0), s(1)))
//    val g_list = ground.collect()
//
//    val js_candidates = jaccard_sim.map(s => (s._1, s._2))
//    val s_list = js_candidates.collect()
//    val true_pos = ground.intersection(js_candidates)
//    val true_pos_list = true_pos.collect()
//
//    // Precision and recall
//    val precision = true_pos_list.length.toDouble / s_list.length.toDouble
//    val recall = true_pos_list.length.toDouble / g_list.length.toDouble
//    println("precision: " + precision.toString)
//    println("recall: " + recall.toString)
  }
}