package Spark_SQL_datasets
import org.apache.spark.sql.SparkSession

import org.apache.log4j._
object wordcount_dataset {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    case class Book(value:String)

    val spark =SparkSession.builder()
      .appName("word_count")
      .master("local[*]").getOrCreate()

    val bookRDD = spark.sparkContext.textFile("datasets/book.txt")
    val word = bookRDD.flatMap(x=>x.split("//W+"))

//    val data_set = word.toDS()

  }

}
