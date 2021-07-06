package Spark_SQL_datasets
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType,StructType,LongType}

object most_popular_movie {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    final case class Movie(movie_id:Int)

    val spark = SparkSession.builder()
      .appName("most_popular_movie")
      .master("local[*]")
      .getOrCreate()

    val schema = new StructType().add("user_id",IntegerType,nullable = false)
      .add("movie_id",IntegerType,nullable = false)
      .add("movie_rating",IntegerType,nullable = false)
      .add("timestamp",LongType,nullable = false)
import spark.implicits._
    val data = spark.read.option("sep","\t")
      .schema(schema)
      .csv("datasets/u.data")

    data.select("movie_id").groupBy("movie_id").count().orderBy(asc("count"))
  }

}
