package Spark_RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object firstSpark {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local").appName("myapp").getOrCreate()
    val data = spark.read.option("sep", "|").option("header", "True").csv("datasets/customer_data.txt")
    data.show()
        val country_data = data.filter(data("Country")==="USA").show()
  }
}
