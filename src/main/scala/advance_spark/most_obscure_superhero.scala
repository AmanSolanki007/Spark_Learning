package advance_spark
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.types.{StructType,StringType,IntegerType}
import org.apache.spark.sql.functions._

object most_obscure_superhero {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    case class superheronames(id:Int,name:String)
    case class superhero(value:String)

    val spark = SparkSession.builder
      .appName("most_obscure_hero")
      .master("local[*]")
      .getOrCreate()

    val superheronameschema = new StructType().add("id",IntegerType,true)
      .add("name",StringType,true)


    val superheroschema = new StructType().add("value",StringType,true)

    import spark.implicits._
    val names = spark.read.option("sep"," ").option("encode","UTF-8")
      .schema(superheronameschema)
      .csv("datasets/Marvel-names.txt")

    val lines = spark.read.schema(superheroschema).text("datasets/Marvel-graph.txt")

    val connections =lines.withColumn("id",split(col("value")," ")(0))
      .withColumn("connections",size(split(col("value")," "))-1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val min_conne_count = connections.agg(min("connections")).first().getLong(0)
    val minConnections = connections.filter($"connections"===min_conne_count)
    val min_conn_name = minConnections.join(names,"id")

    min_conn_name.select("name").show()
  }
}
