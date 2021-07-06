package Spark_RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object wordcount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "wordcount")
    val data = sc.textFile("datasets/book.txt")
    val line = data.flatMap(x => x.split(" "))
    val word_count = line.countByValue().take(5)
    word_count.foreach(println)

  }
}
