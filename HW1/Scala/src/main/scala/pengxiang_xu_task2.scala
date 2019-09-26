import java.io.{BufferedWriter, File, FileWriter}
import System.currentTimeMillis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object pengxiang_xu_task2 {
  def json_parse_review (row: String): (String, Double) ={
    val terms = row.replace("{\"", "")
      .replace("\"}", "")
      .split("(\"?),\"|(\"?\\:\")|(\":)")

    val business_id = terms(5)
    val stars = terms(7)
    (business_id, stars.toDouble)
  }

  def json_parse_business (row: String): (String, String) ={
    val terms = row.replace("{\"", "")
      .replace("\"}", "")
      .split("(\"?),\"|(\"?\\:\")|(\":)")

    val business_id = terms(1)
    val state = terms(9)
    (business_id, state)
  }

  def text_dump(data: Array[(String, Double)]): String ={
    val output_text = new StringBuilder
    output_text ++= "state,stars\n"
    for (line <- data){
      output_text ++= line._1 + "," + line._2.toString() + "\n"
    }
    // Return
    output_text.toString()
  }

  def json_dump (m1: Double,
                 m2: Double,
                 explanation: String
                ): String ={
    val output_text = new StringBuilder
    output_text ++= "{\n"
    output_text ++= "\"m1\": " + m1 + ",\n"
    output_text ++= "\"m2\": " + m2 + ",\n"
    output_text ++= "\"explanation\": \"" + explanation + "\"\n"
    output_text ++= "}"
    // Return
    output_text.toString()
  }

  def write_file(text: String, filename: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }

  def main(args: Array[String]): Unit ={
    val start = currentTimeMillis()

    Logger.getLogger("org").setLevel(Level.FATAL)

    // Run Configuration
    val input_path_r = args(0)
    val input_path_b = args(1)
    val output_path_A = args(2)
    val output_path_B = args(3)
    val ava_cpu_num = Runtime.getRuntime().availableProcessors()
    val task_per_cpu = ava_cpu_num * 2

    // Spark Configuration
    val conf = new SparkConf().setAppName("HW1 - Task 2").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Input Data
    val distFile_r = sc.textFile(input_path_r).coalesce(task_per_cpu)
    val bid_star = distFile_r.map(line => json_parse_review(line))
    val distFile_b = sc.textFile(input_path_b).coalesce(task_per_cpu)
    val bid_state = distFile_b.map(line => json_parse_business(line))

    //    A. What are the average stars for each state? (DO NOT use the stars information in
    //    the business file) (2.5 point)
    val state_star = bid_state.join(bid_star)
    val ss_avg_rdd = state_star
      .map(s => (s._2._1, (s._2._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(s => (s._1, s._2._1 / s._2._2))
      .sortBy(s => (-s._2, s._1))

//    B. You are required to use two ways to print top 5 states with highest stars. You need
//    to compare the time difference between two methods and explain the result within 1 or 2 sentences. (3 point)
//    Method1: Collect all the data, and then print the first 5 states
    var start_B = currentTimeMillis()
    val m1_list = ss_avg_rdd.collect()
    for (a <- 0 to 4){
      print(m1_list(a))
    }
    val m1 = (currentTimeMillis() - start_B) / 1e3d

//      Method2: Take the first 5 states, and then print all
    start_B = currentTimeMillis()
    val m2_list = ss_avg_rdd.take(5)
    m2_list.foreach(println)
    val m2 = (currentTimeMillis() - start_B) / 1e3d
    val explanation = "Method 2 is faster than Method 1, since it only gets the first 5 " +
      "entities verses getting the entire column in Method 1 which takes more time."

    val output_text_A = text_dump(m1_list)
    write_file(output_text_A, output_path_A)
    val output_text_B = json_dump(m1, m2, explanation)
    write_file(output_text_B, output_path_B)

    val duration = (currentTimeMillis() - start) / 1e3d
    println("Task2 runtime : " + duration)
  }
}