package Spark_RDD
import org.apache.spark.SparkContext
import org.apache.log4j._

object test123 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","test")

    def countword(url:String):Map[String,Long]={
      val data = sc.textFile(url)
      data.flatMap(x=>x.split(" ")).countByValue().take(5).toMap
    }
println(countword("datasets/book.txt"))

  }

}
