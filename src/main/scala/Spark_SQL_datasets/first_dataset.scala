package Spark_SQL_datasets
import org.apache.spark.sql._
import org.apache.log4j._
object first_dataset {
  case class person(userID:Int,name:String,age:Int,friends:Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("muapp")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val data = spark.read.option("header","true")
      .option("inferschema","true")
      .csv("datasets/fakefriends-header.csv")
      .as[person]

    data.printSchema()

     data.createOrReplaceTempView("person")
     spark.sql("select distinct(*) from person").show()

     data.select("name").show()
     data.select("name","age").groupBy("age").count().show()
     data.filter(data("age") < 65).show()

    spark.stop()

  }
}
