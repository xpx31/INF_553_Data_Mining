import java.io.{BufferedWriter, File, FileWriter}
import System.nanoTime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object pengxiang_xu_task2 {
  def write_file(data: Array[((Int, Int), (Double, Double))],
                 user: Array[String],
                 business: Array[String],
                 output_path: String): Unit = {
    val file = new File(output_path)
    val bw = new BufferedWriter(new FileWriter(file))
    var output_text = new StringBuilder

    for (line <- data){
      output_text ++= user(line._1._1) + "," + business(line._1._2) + "," + line._2._2.toString + "\n"
    }

    bw.write(output_text.toString())
  }

  //  def extract_data(line: ((Int, Int), (Double, Double)),
  //                   user: Array[String],
  //                   business: Array[String]) : String ={
  //    user(line._1._1) + "," + business(line._1._2) + "," + line._2._2.toString + "\n"
  //  }

  // Cut off ratings to be within range
  def cut_rating(rating: ((Int, Int), Double)): ((Int, Int), Double) ={
    var rate = 0.0
    if (rating._2 > 5) {rate = 5.0}
    else if (rating._2 < 1) {rate = 1.0}
    else {rate = rating._2}

    (rating._1, rate)
  }

  // Model based cf
  def model_base_cf(rdd: RDD[Array[String]],
                    train_rdd: RDD[Array[String]],
                    test_rdd: RDD[Array[String]],
                    user_map: mutable.HashMap[String, Int],
                    bus_map: mutable.HashMap[String, Int]): RDD[((Int, Int), (Double, Double))] ={
    // Training
    val ratings_train = train_rdd
      .map(s => Rating(user_map(s(0)), bus_map(s(1)), s(2).toDouble))
    val rank = 3
    val num_iter = 20
    val model = ALS.train(ratings_train, rank, num_iter, 0.265)

    // Evaluation
    val test_features = test_rdd
      .map(s => (user_map(s(0)), bus_map(s(1))))
    val prediction = model
      .predict(test_features)
      .map{case Rating(user, product, rating) => ((user, product), rating.toDouble)}
      .map(cut_rating)

    // Combine test and prediction
    val ratings_test = test_rdd
      .map(s => Rating(user_map(s(0)), bus_map(s(1)), s(2).toDouble))
    var rate_pred = ratings_test
      .map{case Rating(user, product, rating) => ((user, product), rating.toDouble)}
      .join(prediction)
      .cache()

    // Filling users in test and not in training into prediction by default rating, 3
    val d_rating = 3.0
    val rdd_u = rdd.map(s => (user_map(s(0)), (bus_map(s(1)), s(2).toDouble)))
    val rp_u = train_rdd.map(s => (user_map(s(0)), (bus_map(s(1)), s(2).toDouble)))
    val rate_pred_u = rdd_u.subtractByKey(rp_u)
      .map(s => ((s._1, s._2._1), (s._2._2, d_rating)))

    // Filling businesses in test and not in training into prediction by default rating, 3
    val rdd_b = train_rdd.map(s => (bus_map(s(1)), (user_map(s(0)), s(2).toDouble)))
    val rp_b = train_rdd.map(s => (bus_map(s(1)), (user_map(s(0)), s(2).toDouble)))
    val rate_pred_b = rdd_b.subtractByKey(rp_b)
      .map(s => ((s._2._1, s._1), (s._2._2, d_rating)))

    // Combine all
    val rate_pred_diff = rate_pred_u.union(rate_pred_b)
    rate_pred = rate_pred.union(rate_pred_diff)

    rate_pred
  }

  def user_base_cf(case_num: Int,
                   train_rdd: RDD[Array[String]],
                   test_rdd: RDD[Array[String]],
                   user_map: mutable.HashMap[String, Int],
                   bus_map: mutable.HashMap[String, Int]): RDD[((Int, Int), (Double, Double))] ={
    // Business to user map
    val bus_user_map = train_rdd
      .map(s => (bus_map(s(1)), List(user_map(s(0)))))
      .reduceByKey((x, y) => x ++ y)
      .collectAsMap()

    // User Information
    val user_bus = train_rdd
      .map(s => (user_map(s(0)), List(bus_map(s(1)))))
      .reduceByKey((x, y) => x ++ y)
    val user_rating = train_rdd
      .map(s => (user_map(s(0)), List(s(2).toDouble)))
      .reduceByKey((x, y) => x ++ y)
      .map(s => normalize_rating(s))
    val user_bus_rating = user_bus.join(user_rating)
      .map(s => ((s._1, s._2._1), (s._2._2._1, s._2._2._2)))

    // User to business map
    val user_bus_map = user_bus.collectAsMap()

    // User and business to ratings map
    val user_bus_rating_map = user_bus_rating
      .flatMap(s => expand_ratings(case_num, s))
      .collectAsMap()

    // Test Features
    val test_features = test_rdd.map(s => (user_map(s(0)), bus_map(s(1))))

    // Take top n number of cos similarities
    val top = 13

    // Prediction
    val prediction = test_features
      .map(s => predict_rating(case_num, s, user_bus_rating_map, user_bus_map, bus_user_map, top))
      .map(cut_rating)
    val rate_pred = test_rdd
      .map(s => ((user_map(s(0)), bus_map(s(1))), s(2).toDouble))
      .join(prediction)

    rate_pred
  }

  def item_base_cf(case_num: Int,
                   train_rdd: RDD[Array[String]],
                   test_rdd: RDD[Array[String]],
                   user_map: mutable.HashMap[String, Int],
                   bus_map: mutable.HashMap[String, Int]): RDD[((Int, Int), (Double, Double))] ={
    // User to business map
    val user_bus_map = train_rdd
      .map(s => (user_map(s(0)), List(bus_map(s(1)))))
      .reduceByKey((x, y) => x ++ y)
      .collectAsMap()

    // Business Information
    val bus_user = train_rdd
      .map(s => (bus_map(s(1)), List(user_map(s(0)))))
      .reduceByKey((x, y) => x ++ y)
    val bus_rating = train_rdd
      .map(s => (bus_map(s(1)), List(s(2).toDouble)))
      .reduceByKey((x, y) => x ++ y)
      .map(s => normalize_rating(s))
    val user_bus_rating = bus_user.join(bus_rating)
      .map(s => ((s._1, s._2._1), (s._2._2._1, s._2._2._2)))

    // User to business map
    val bus_user_map = bus_user.collectAsMap()

    // User and business to ratings map
    val user_bus_rating_map = user_bus_rating
      .flatMap(s => expand_ratings(case_num, s))
      .collectAsMap()

    // Test Features
    val test_features = test_rdd.map(s => (user_map(s(0)), bus_map(s(1))))

    // Take top n number of cos similarities
    val top = 6

    // Prediction
    val prediction = test_features
      .map(s => predict_rating(case_num, s, user_bus_rating_map, user_bus_map, bus_user_map, top))
      .map(cut_rating)

    val rate_pred = test_rdd
      .map(s => ((user_map(s(0)), bus_map(s(1))), s(2).toDouble))
      .join(prediction)

    rate_pred
  }


  // Get RMSE value
  def evaluate_rmse(rate_pred: RDD[((Int, Int), (Double, Double))]): Double ={
    val rmse = math.sqrt(rate_pred.coalesce(12)
      .map(s => math.pow(s._2._1 - s._2._2, 2))
      .mean()
    )
    println("RMSE: " + rmse.toString)
    rmse
  }

  // Normalize rating for each user or business
  def normalize_rating(data: (Int, List[Double])) : (Int, (List[Double], Double)) ={
    // Field could be a user or business
    val field = data._1
    val ratings_old = data._2
    var ratings_avg = 0.0

    // Get avereage rating
    for (r <- ratings_old) {
      ratings_avg += r.toDouble
    }

    ratings_avg /= ratings_old.length
    (field, (ratings_old, ratings_avg))
  }

  def expand_ratings(case_num: Int,
                     data: ((Int, List[Int]), (List[Double], Double))) : List[((Int, Int), (Double, Double))] ={
    val f_single = data._1._1
    val f_list = data._1._2
    val ratings = data._2._1
    val avg_rating = data._2._2

    var expand_list = new ListBuffer[((Int, Int), (Double, Double))]()
    if (case_num == 2) {
      // User base cf
      for (i <- f_list.indices) {
        // ((user, business), (rating, avg. rating))
        expand_list += (((f_single, f_list(i)), (ratings(i), avg_rating)))
      }
    } else if (case_num == 3){
      // Item base cf
      for (i <- f_list.indices) {
        expand_list += (((f_list(i), f_single), (ratings(i), avg_rating)))
      }
    }

    expand_list.toList
  }

  def predict_rating(case_num: Int,
                     test_fea: (Int, Int),
                     ubr_map: scala.collection.Map[(Int, Int), (Double, Double)],
                     ub_map: scala.collection.Map[Int, List[Int]],
                     bu_map: scala.collection.Map[Int, List[Int]],
                     top_n: Int) : ((Int, Int), Double) ={
    val user = test_fea._1.toInt
    val busi = test_fea._2.toInt

    // Business in testing but not in training
    if (!bu_map.contains(busi) && !ub_map.contains(user)){
      ((user, busi), 3.0)
    } else if (!bu_map.contains(busi)) {
      // Average of the other businesses that the user rated
      ((user, busi), ubr_map((user, ub_map(user).head))._2)
    } else if (!ub_map.contains(user)){
      // Average of the business rating from other users
      ((user, busi), ubr_map((bu_map(busi).head, busi))._2)
    } else {
      // Business in training as well
      var field_list: List[Int] = List()
      var co_rate: List[Int] = List()
      var field_avg = 0.0

      if (case_num == 2) {
        // user base cf
        field_list = ub_map(user)
        field_avg = ubr_map((user, field_list.head))._2
        co_rate = bu_map(busi)
      } else if (case_num == 3) {
        // item base cf
        field_list = bu_map(busi)
        field_avg = ubr_map((field_list.head, busi))._2
        co_rate = ub_map(user)
      }

      val cos_sims = get_cos_sims(case_num, co_rate, field_list, field_avg, ubr_map, ub_map, bu_map, user, busi, top_n)
      val rate_pred = get_rating_pred_cs(case_num, cos_sims, ubr_map, field_avg, user, busi)

      rate_pred
    }
  }

  def get_cos_sims(case_num: Int,
                   co_rate: List[Int],
                   field_list: List[Int],
                   rate_avg: Double,
                   ubr_map: scala.collection.Map[(Int, Int), (Double, Double)],
                   ub_map: scala.collection.Map[Int, List[Int]],
                   bu_map: scala.collection.Map[Int, List[Int]],
                   user: Int,
                   busi: Int,
                   top_n: Int) : List[(Int, Double, Double)] ={
    var cos_sims_buff = new ListBuffer[(Int, Double, Double)]
    var rate_avg_2 = 0.0
    var user_co_rate_len = 0
    var bus_co_rate_len = 0

    // Find cos similarities of the co-rated users or business
    for (f <- co_rate){
      var other_field_co_rate = field_list.toSet
      if (case_num == 2) {
        // user base cf
        other_field_co_rate &= ub_map(f).toSet
        user_co_rate_len = other_field_co_rate.size
        rate_avg_2 = ubr_map((f, ub_map(f).head))._2
      } else if (case_num == 3) {
        // item base cf
        other_field_co_rate &= bu_map(f).toSet
        bus_co_rate_len = other_field_co_rate.size
        rate_avg_2 = ubr_map((bu_map(f).head, f))._2
      }

      var num_cs = 0.0
      var den_cs_1 = 0.0
      var den_cs_2 = 0.0

      // Calculate the numerator and denominator of each cos similarity
      for (of <- other_field_co_rate){
        var r_1 = 0.0
        var r_2 = 0.0
        if (case_num == 2) {
          // user base cf
          if (of != busi) {
            r_1 = ubr_map((user, of))._1 - rate_avg
            r_2 = ubr_map((f, of))._1 - rate_avg_2
          }
        } else if (case_num == 3) {
          if (of != user) {
            r_1 = ubr_map((of, busi))._1 - rate_avg
            r_2 = ubr_map((of, f))._1 - rate_avg_2
          }
        }

        num_cs += r_1 * r_2
        den_cs_1 += math.pow(r_1, 2)
        den_cs_2 += math.pow(r_2, 2)
      }

      // Calculate cos. similarity
      var cos_sim = 0.0
      if (num_cs != 0.0) {
        cos_sim = num_cs / math.sqrt(den_cs_1 * den_cs_2)
      }

      // Memory-based improvement
      if (case_num == 2 && user_co_rate_len < 20){
        cos_sim = 0.0
      }else if(case_num == 3 && bus_co_rate_len < 60){
        cos_sim = 0.0
      }

      cos_sims_buff += ((f, cos_sim, rate_avg_2))
    }

    // Take the top n neighbors
    var cos_sims = cos_sims_buff.toList
    cos_sims.sortBy(s => s._2)
    cos_sims = cos_sims.take(top_n)

    cos_sims
  }

  def get_rating_pred_cs(case_num: Int,
                         cos_sims: List[(Int, Double, Double)],
                         ubr_map: scala.collection.Map[(Int, Int), (Double, Double)],
                         rate_avg: Double,
                         user: Int,
                         busi: Int
                        ) : ((Int, Int), Double) = {
    var num_w = 0.0
    var den_w = 0.0
    var r = 0.0

    // Get weights
    for (cs <- cos_sims) {
      val field_cs = cs._1
      val cos_sim = cs._2
      val rating_cs = cs._3

      if (case_num == 2) {
        // User based cf
        // rating_cs is average rating of business coloumn for every other users
        if (ubr_map.contains((field_cs, busi))){
          r = ubr_map((field_cs, busi))._1 - rating_cs
        }
      } else if (case_num == 3) {
        // Item based cf
        // r is the rating of business for every other user
        r = ubr_map((user, field_cs))._1
      }

      num_w += r * cos_sim
      den_w += math.abs(cos_sim)
    }

    // Weighted rating
    var rating_pred = rate_avg
    if (den_w != 0.0) {
      if (case_num == 2){
        rating_pred = rate_avg + num_w / den_w
      } else if (case_num == 3) {
        rating_pred = num_w / den_w
      }
    }

    ((user, busi), rating_pred)
  }

  def main(args: Array[String]): Unit = {
    val start = nanoTime()

    Logger.getLogger("org").setLevel(Level.FATAL)

    // Run Configuration
    val train_path = args(0)
    val test_path = args(1)
    val case_id = args(2).toInt
    val output_path = args(3)
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
    val distFile_train = sc.textFile(train_path, 2)
      .coalesce(task_per_cpu)
    val distFile_test = sc.textFile(test_path, 2)
      .coalesce(task_per_cpu)

    // Beheading
    val headers = distFile_train.first
    val rdd_train = distFile_train
      .filter(_ != headers)
      .map(s => s.split(","))
      .persist()
    val rdd_test = distFile_test
      .filter(_ != headers)
      .map(s => s.split(","))
      .persist()

    // All users and businesses from both training and testing rdds
    val rdd = sc.union({rdd_train}, {rdd_test})
    val user = rdd.map(s => s(0)).distinct().collect()
    val business = rdd.map(s => s(1)).distinct().collect()

    // Constructing dictionaries for users and business index
    val user_map: mutable.HashMap[String, Int] = mutable.HashMap()
    var i = 0
    for (u <- user) {
      user_map.update(u, i)
      i += 1
    }

    val bus_map: mutable.HashMap[String, Int] = mutable.HashMap()
    i = 0
    for (b <- business) {
      bus_map.update(b, i)
      i += 1
    }

    // Result
    val rst: RDD[((Int, Int), (Double, Double))] = {
      // Model Based CF
      if (case_id == 1) {
        model_base_cf(rdd, rdd_train, rdd_test, user_map, bus_map)
      } else if (case_id == 2) {
        user_base_cf(case_id, rdd_train, rdd_test, user_map, bus_map)
      } else {
        item_base_cf(case_id, rdd_train, rdd_test, user_map, bus_map)
      }
    }.coalesce(task_per_cpu).persist()

    write_file(rst.collect(), user, business, output_path)

    val duration = (nanoTime() - start) / 1e9
    println("Duration: " + duration)

    //    evaluate_rmse(rst)
  }
}