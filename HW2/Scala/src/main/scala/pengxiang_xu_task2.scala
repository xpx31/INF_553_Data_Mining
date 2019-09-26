import java.io.{BufferedWriter, File, FileWriter}
import System.nanoTime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.control.Breaks

object pengxiang_xu_task2 {
  def write_file(text: String, filename: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }

  def record_data(sorted_data: Array[List[String]]): String = {
    val output_text = new StringBuilder
    var last_size = 0

    for (list <- sorted_data) {
      val cur_size = list.size
      if (cur_size > last_size) {
        if (last_size != 0) {
          output_text ++= "\n\n"
        }
        last_size = cur_size
      } else {
        output_text ++= ","
      }

      // Items
      output_text ++= "("
      for (i <- list.indices){
        output_text ++= "\'" + list(i) + "\'"
        if (i < list.length - 1){
          output_text ++= ", "
        }
      }
      output_text ++= ")"
    }

    output_text.toString()
  }


  def sort_list(l1: List[String], l2: List[String]): Boolean ={
    // Compare sizes
    if (l1.size > l2.size){
      return false
    } else if (l1.size < l2.size){
      return true
    }

    // Compare each string
    var i1 = 0
    var i2 = 0
    while (i1 < l1.size && i2 < l2.size){
      if (l1(i1) > l2(i2)){
        return false
      } else if (l1(i1) < l2(i2)){
        return true
      } else{
        i1 += 1
        i2 += 1
      }
    }

    true
  }

  def get_grouped_data(raw_rdd: RDD[String]): RDD[Iterable[String]] ={
    val data_p: RDD[(String, String)] = raw_rdd.map(s =>
      (s.split(",")(0), s.split(",")(1)))

    val data_grouped: RDD[Iterable[String]] = data_p
      .map(s => (s._1, s._2))
      .groupByKey()
      .values
      .persist()

    data_grouped
  }

  def generate_permutation(iterable : List[List[String]], num_set: Int): List[List[String]] ={
    val inner = new Breaks
    var permutations: List[List[String]]  = List[List[String]]()
    val min_set_size = 2

    var iter = List[String]()
    // All permutations of pairs of two - order 1's
    if (num_set == min_set_size){
      for (list <- iterable){
        for (x <- list){
          iter ++= List(x)
        }
      }
      permutations ++= iter.combinations(min_set_size)
    } else {
      for (i <- iterable.indices) {
        val item_cur = iterable(i)
        val item_cur_set = item_cur.toSet

        // Pair each item with other item(s)
        inner.breakable {
          for (j <- i + 1 until iterable.size) {
            val item_next = iterable(j)
            // Higher order pairs
            // Adding new permutation of two frequent itemsets
            if (item_cur.take(num_set - min_set_size).equals
            (item_next.take(num_set - min_set_size))) {
              permutations ++=
                List[List[String]](item_cur_set.union(item_next.toSet).toList.sorted)
            } else {
              inner.break
            }
          }
        }
      }
    }

    permutations
  }

  def get_freq_candidates(baskets: List[Iterable[String]],
                          support_th: Double): List[List[String]] = {
    val counter = mutable.HashMap[String, Int]().empty
    for (basket <- baskets) {
      // Find count of each item from each basket
      for (item <- basket) {
        val i_set = item
        counter(i_set) = counter.getOrElseUpdate(i_set, 0) + 1
      }
    }

    var freq_candidates = List[List[String]]()
    counter.filter(s => s._2 >= support_th).keys
      .foreach(s => freq_candidates ++= List(List(s)))
    freq_candidates.sortWith(sort_list)
  }

  def get_freq_candidates(baskets: List[Iterable[String]],
                          support_th: Double,
                          permutation_list: List[List[String]],
                          num_set: Int): List[List[String]] ={
    if (permutation_list.isEmpty) {
      return List[List[String]]()
    }

    val counter = mutable.HashMap[List[String], Int]()
    for (basket <- baskets){
      val basket_itemset = basket.toSet
      // Find count of each itemset from each basket
      for(permutation <- permutation_list) {
        val p_set = permutation.toSet
        if (p_set.subsetOf(basket_itemset)){
          counter(permutation) = counter.getOrElseUpdate(permutation, 0) + 1
        }
      }
    }

    var freq_candidates = List[List[String]]()
    counter.filter(s => s._2 >= support_th).keys
      .foreach(s => freq_candidates ++= List[List[String]](s))
    freq_candidates.sortWith(sort_list)
  }


  def apriori(baskets: Iterator[Iterable[String]],
              count: Double,
              threshold: Int): Iterator[List[String]] = {
    val b_list = baskets.toList
    var candidate_list: List[List[String]] = List[List[String]]()
    val support_th = threshold * (b_list.size.toDouble / count)
    var permutation_list = List[List[String]]()

    // Pass 1
    var frequent_candidates = get_freq_candidates(b_list, support_th)

    // Pass n
    var n = 2
    while (frequent_candidates.nonEmpty) {
//      start = nanoTime()
      candidate_list ++= frequent_candidates
//      println("candidate list: " + ((nanoTime() - start) / 1e9).toString)
      //      println("Candidate_list: ")
      //      candidate_list.foreach(println)

//      start = nanoTime()
      permutation_list = generate_permutation(frequent_candidates, n)
//      println("permutation list: " + ((nanoTime() - start) / 1e9).toString)
      // Iterator can only traverse once
      //      println("Permutation_list: ")
      //      permutation_list.foreach(println)

//      start = nanoTime()
      frequent_candidates = get_freq_candidates(b_list, support_th, permutation_list, n)
//      println("frequent candidate: " + ((nanoTime() - start) / 1e9).toString)
      //      println("Frequent_candidate: ")
      //      frequent_candidates.foreach(println)

      n += 1
    }

    candidate_list.toIterator
  }

  def count_freq(baskets: Iterator[Iterable[String]],
                 candidates: Array[List[String]]): Iterator[(List[String], Int)] ={
    val b_list = baskets.toList
    var freq_items: List[(List[String], Int)] = List[(List[String], Int)]()

    // Calculate the count of each candidate in basket
    for (candidate <- candidates){
      var count = 0
      val c_set = candidate.toSet
      for (basket <- b_list){
        val b_set = basket.toSet
        if (c_set.subsetOf(b_set)){
          count += 1
        }
      }

      freq_items ++= List((candidate, count))
    }
    freq_items.toIterator
  }

  def main(args: Array[String]): Unit = {
    val start = nanoTime()

    Logger.getLogger("org").setLevel(Level.FATAL)

    // Run Configuration
    val filter_th = args(0).toInt
    val support = args(1).toInt
    val input_path = args(2)
    val output_path = args(3)
    val ava_cpu_num = Runtime.getRuntime.availableProcessors()
    val task_per_cpu = ava_cpu_num * 3

    // Spark Configuration
    val conf = new SparkConf().setAppName("HW2 - Task 2").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Input Data
    val distFile = sc.textFile(input_path, minPartitions = 2).coalesce(task_per_cpu)

    // SON Algorithm
    val headers = distFile.first
    val data = distFile.filter(_ != headers)
    val groupded_data = get_grouped_data(data).filter(s => s.size > filter_th)
//    println(groupded_data.collect().foreach(println))

    val num_count = groupded_data.count().toDouble
    val num_part = groupded_data.getNumPartitions

    // Pass 1
    val candidates = groupded_data
      .mapPartitions(b => apriori(b, num_count, support))
      .map(s => (s, 1))
      .reduceByKey((x, y) => 1)
      .keys
      .collect()
    val candidates_sorted = candidates.sortWith(sort_list)

    // Pass 2
    val freq_itemsets = groupded_data
      .mapPartitions(b => count_freq(b, candidates))
      .reduceByKey((x, y) => x + y)
      .filter(s => s._2 >= support)
      .map(s => s._1)
      .collect()
    val freq_itemset_sorted = freq_itemsets.sortWith(sort_list)

    //    println("Candidates: ")
    //    candidates_sorted.foreach(s => println(s))

//    println("Frequent Itemsets: ")
//    freq_itemset_sorted.foreach(s => println(s))

    // Output Data
    var output_text = "Candidates:\n"
    output_text ++= record_data(candidates_sorted)
    output_text ++= "\n\n"
    output_text ++= "Frequent Itemsets:\n"
    output_text ++= record_data(freq_itemset_sorted)
    write_file(output_text, output_path)

    val duration = (nanoTime() - start) / 1e9
    println("Duration: " + duration)
  }
}
