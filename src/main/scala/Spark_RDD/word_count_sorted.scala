package Spark_RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object word_count_sorted {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "word_count_sorted")
    val data = sc.textFile("datasets/book.txt")
    val line = data.flatMap(x => x.split("\\W+"))
    val lower_case = line.map(x => x.toLowerCase)
    val wordcount = lower_case.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    val swap = wordcount.map(x => (x._2, x._1)).sortByKey()
    for (i <- swap) {
      val count = i._1
      val word = i._2
      println(s"$word:$count")
    }

  }
}
