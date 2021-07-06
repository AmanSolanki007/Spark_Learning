package Spark_RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object number_of_friends {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "myapp")
    val data = sc.textFile("datasets/fakefriends.csv")

    def lineparser(line: String) = {
      val field = line.split(",")
      val age = field(2).toInt
      val num_of_friends = field(3).toInt
      (age, num_of_friends)
    }

    val rdd = data.map(lineparser)
    val totalbyage = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val averagesbyage = totalbyage.mapValues(x => x._1 / x._2)
    val result = averagesbyage.take(5)
    for (i <- result) println(i)


  }

}
