package Spark_RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object count_ratings {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "myapp")
    val lines = sc.textFile("datasets/u.data")
    val ratings = lines.map(x => x.split("\t")(2))
    val count_rating = ratings.countByValue()
    val rdd = count_rating.take(5)
    println(rdd)
  }
}
