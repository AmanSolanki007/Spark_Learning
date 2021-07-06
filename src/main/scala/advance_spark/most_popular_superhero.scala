package advance_spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._

object most_popular_superhero {
  def main(args: Array[String]): Unit = {

    case class superher_names(id: Int, name: String)
    case class superhero(value: String)

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("most_popular_hero")
      .getOrCreate()

    val schema = new StructType().add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    val schema_1 = new StructType().add("value", StringType, nullable = true)

    import spark.implicits._

    val S_name = spark.read
      .schema(schema).option("sep"," ")
      .csv("datasets/Marvel-names.txt")

    val S_lines =
      spark.read.schema(schema_1).text("datasets/Marvel-graph.txt")

    val connections = S_lines.withColumn("id", split(col("value"), "")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val mostpopular = connections.sort($"connections".desc).first()

    val mostpopularname = S_name.filter($"id"===mostpopular(0)).select("name").first()
    println(mostpopularname(0),mostpopular(1))

  }
}
