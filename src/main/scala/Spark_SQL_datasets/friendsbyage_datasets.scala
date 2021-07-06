package Spark_SQL_datasets
import org.apache.spark.sql._
import org.apache.log4j._

object friendsbyage_datasets {
  case class Person(userID:Int,name:String,age:Int,friends:Int)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("friend_by_age")
      .getOrCreate()

    import spark.implicits._

    val data = spark.read.option("header","True")
      .option("inferschema","True")
      .csv("datasets/fakefriends-header.csv")
      .as[Person]

    data.printSchema()
    data.select("age","friends")
      .groupBy("age")
      .sum("friends").sort("age")
      .alias("friends_by_age").show()

  }
}
