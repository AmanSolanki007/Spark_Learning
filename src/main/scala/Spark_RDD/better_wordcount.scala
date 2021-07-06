package Spark_RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object better_wordcount {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Wordcount")
    val data = sc.textFile("datasets/book.txt")
    val line = data.flatMap(x => x.split("\\W+"))
    val lover = line.map(x => x.toLowerCase)
    val wordcount = lover.countByValue()
    wordcount.foreach(println)
  }
}
