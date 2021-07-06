package Spark_RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object total_expenditure {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "total_expenditure")
    val data = sc.textFile("datasets/customer-orders.csv")

    def lineparser(line: String) = {
      val field = line.split(",")
      val customer_id = field(0)
      val total_expenditure = field(2).toFloat
      (customer_id, total_expenditure)
    }

    val line = data.map(lineparser)
    val count = line.reduceByKey((x, y) => (x + y)).sortByKey()
    count.foreach(println)
  }
}
