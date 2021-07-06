package Spark_RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object filter_rdd {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "myapp")
    val data = sc.textFile("datasets/1800.csv")

    def lineparser(line: String) = {
      val field = line.split(",")
      val StationID = field(0)
      val entrytype = field(2)
      val temperature = field(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
      (StationID, entrytype, temperature)
    }

    val rdd = data.map(lineparser)
    val result = rdd.filter(x => x._2 == "TMAX").collect()
    for (i <- result) println(i)

    val stationTemp = rdd.map(x => (x._1, x._3.toFloat))

    val mintemp = rdd.filter(x => x._2 == "TMIN")

    for (i <- stationTemp) println(i)


    val min_station_temp = stationTemp.reduceByKey((x, y) => math.min(x, y)).collect()
    for (i <- min_station_temp) println(i)


  }
}
