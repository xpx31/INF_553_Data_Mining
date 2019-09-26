import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{BufferedWriter, File, FileWriter}
import System.currentTimeMillis

object pengxiang_xu_task1 {
  def json_parse (row: String): (String, String, Double, Int, String) ={
    val terms = row.replace("{\"", "")
      .replace("\"}", "")
      .split("(\"?),\"|(\"?\\:\")|(\":)")

    val user_id = terms(3)
    val business_id = terms(5)
    val stars = terms(7)
    val useful = terms(9)
    val text = terms(15)
      .replace("\\n", "")
      .replace("\\", "")
    (user_id, business_id, stars.toDouble, useful.toInt, text)
  }

  def json_dump (n_review_useful: Long,
                 n_review_5_star: Long,
                 n_characters: Long,
                 n_user: Long,
                 top20_user: Array[(String, Int)],
                 n_business: Long,
                 top20_business: Array[(String, Int)]
                ): String ={
    val output_text = new StringBuilder
    output_text ++= "{\n"
    output_text ++= "\"n_review_useful\": " + n_review_useful + ",\n"
    output_text ++= "\"n_review_5_star\": " + n_review_5_star + ",\n"
    output_text ++= "\"n_characters\": " + n_characters + ",\n"
    output_text ++= "\"n_user\": " + n_user + ",\n"
    output_text ++= top20_fill("top20_user", top20_user) + ",\n"
    output_text ++= "\"n_business\": " + n_business + ",\n"
    output_text ++= top20_fill("top20_business", top20_business)
    output_text ++= "\n}"
    // Return
    output_text.toString()
  }

  def top20_fill (top20_type: String, top20_data: Array[(String, Int)]): String ={
    val top20_text = new StringBuilder
    top20_text ++= "\"" + top20_type + "\": ["
    for (record <- top20_data) {
      top20_text ++= "[\"" + record._1 + "\", "
      top20_text ++= record._2 + "],"
    }
    top20_text.deleteCharAt(top20_text.length - 1)
    top20_text ++= "]"
    // Return
    top20_text.toString()
  }

  def write_file(json_text: String, filename: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(json_text)
    bw.close()
  }

  def main(args: Array[String]): Unit ={
    val start = currentTimeMillis()
    Logger.getLogger("org").setLevel(Level.FATAL)

    // Run Configuration
    val input_path = args(0)
    val output_path = args(1)
    val ava_cpu_num = Runtime.getRuntime().availableProcessors()
    val task_per_cpu = ava_cpu_num * 2

    // Spark Configuration
    val conf = new SparkConf().setAppName("HW1 - Task 1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Input Data
    val distFile = sc.textFile(input_path).coalesce(task_per_cpu)
    val rdd = distFile.map(line => json_parse(line))

//    A. The number of reviews that people think are useful (The value of tag ‘useful’ > 0)
//    (1 point)
    val n_review_useful = rdd.filter(s => s._4 > 0).count()

//    B. The number of reviews that have 5.0 stars rating (1 point)
    val n_review_5_star = rdd.filter(s => s._3 == 5.0).count()

//    C. How many characters are there in the ‘text’ of the longest review (1 point)
    val n_characters = rdd.map(s => s._5.length()).max()

//    D. The number of distinct users who wrote reviews (1 point)
    val users = rdd.map(s => s._1)
    val n_user = users.distinct().count()

//    E. The top 20 users who wrote the largest numbers of reviews and the number of
//    reviews they wrote (1 point)
    val u_user = users.map(s => (s, 1)).reduceByKey(_+_)
    val top20_user = u_user.sortBy(s => (-s._2, s._1)).take(20)

//    F. The number of distinct businesses that have been reviewed (1 point)
    val business = rdd.map(s => s._2)
    val n_business = business.distinct().count()

//    G. The top 20 businesses that had the largest numbers of reviews and the number of
//    reviews they had (1 point)
    val u_business = business.map(s => (s, 1)).reduceByKey(_+_)
    val top20_business = u_business.sortBy(s => (-s._2, s._1)).take(20)

    // Output Data
    val json_output = json_dump(n_review_useful,
      n_review_5_star,
      n_characters,
      n_user,
      top20_user,
      n_business,
      top20_business)
    write_file(json_output, output_path)
    println("Task1 runtime: " + (currentTimeMillis() - start) / 1e3d)
  }
}
